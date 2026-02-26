"""
FastAPI endpoints for the Distress Scanner.

Run with: uvicorn src.api.app:app --reload --port 8001
Dashboard: http://localhost:8001/dashboard
"""

from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import structlog

from src.analysis.scanner import scan_distress, scan_free, enrich_sentinel, rescore_with_sentinel
from src.naip.baseline import naip_baseline
from src.fema.flood import fema_flood
from src.planet.client import planet_search, planet_refine
from src.landsat.client import landsat_trends
from src.db import (get_db_connection, migrate_add_scan_columns,
                    migrate_add_sentinel_columns, migrate_add_usps_columns,
                    migrate_add_planet_columns, migrate_add_composite_columns,
                    migrate_add_conviction_columns,
                    batch_update_sentinel_results,
                    get_planet_scan_date, set_planet_scan_date,
                    update_parcel_planet,
                    get_usps_cache, save_usps_check, update_parcel_usps)
from src.usps.vacancy import USPSVacancyChecker, split_situs
from src.analysis.flags import evaluate_usps_vacancy

logger = structlog.get_logger("api")

# Feature flags
PLANET_ENABLED = os.environ.get("PLANET_ENABLED", "false").lower() == "true"

app = FastAPI(
    title="Distress Scanner API",
    description="Satellite imagery analysis for property distress signals",
    version="0.2.0",
)


@app.on_event("startup")
def run_migrations():
    """Run DB migrations on startup to ensure scan + sentinel columns exist."""
    try:
        conn = get_db_connection()
        migrate_add_scan_columns(conn)
        migrate_add_sentinel_columns(conn)
        migrate_add_usps_columns(conn)
        migrate_add_planet_columns(conn)
        migrate_add_composite_columns(conn)
        migrate_add_conviction_columns(conn)
        conn.close()
        logger.info("startup_migration_ok")
    except Exception as e:
        logger.warning("startup_migration_skipped", error=str(e))

# CORS for Netlify / external dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DASHBOARD_PATH = Path(__file__).parent.parent / "dashboard" / "index.html"


@app.get("/health")
def health():
    return {"status": "ok", "service": "distress-scanner", "version": "0.2.0"}


@app.get("/config")
def config():
    """Return client-safe config. Only browser-safe keys — never serve server keys here."""
    return {
        "google_maps_key": os.environ.get("GOOGLE_MAPS_BROWSER_KEY", ""),
        "planet_enabled": PLANET_ENABLED,
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Serve the Command Center dashboard."""
    if DASHBOARD_PATH.exists():
        return HTMLResponse(DASHBOARD_PATH.read_text())
    raise HTTPException(status_code=404, detail="Dashboard not found")


@app.get("/scan_distress")
def api_scan_distress(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lng: float = Query(..., ge=-180, le=180, description="Longitude"),
    months: int = Query(12, ge=1, le=36, description="Sentinel lookback months"),
    parcel_id: str = Query(None, description="Parcel ID for Planet re-run guard"),
    county: str = Query(None, description="County for Planet re-run guard"),
    force_planet: bool = Query(False, description="Force Planet re-scan even if recent"),
):
    """Full distress scan: NAIP + Sentinel/Landsat + FEMA + Planet."""
    try:
        # Planet feature-flag: skip entirely if disabled
        skip_planet = not PLANET_ENABLED
        planet_skip_reason = "Planet is feature-flagged off (PLANET_ENABLED=false)." if skip_planet else None
        if not skip_planet and parcel_id and county and not force_planet:
            try:
                from datetime import datetime, timedelta
                conn = get_db_connection()
                last_planet = get_planet_scan_date(conn, parcel_id, county)
                conn.close()
                if last_planet and (datetime.now() - last_planet).days < 60:
                    skip_planet = True
                    planet_skip_reason = f"Planet scanned {(datetime.now() - last_planet).days} days ago. Pass force_planet=true to override."
                    logger.info("planet_skipped_recent", parcel_id=parcel_id,
                                last_scan=last_planet.isoformat(),
                                days_ago=(datetime.now() - last_planet).days)
            except Exception as e:
                logger.warning("planet_guard_check_failed", error=str(e))

        result = scan_distress(lat, lng, months=months, skip_planet=skip_planet)

        # Stamp planet_scan_date + persist full Planet results only if Planet succeeded
        planet_data = result.get("planet")
        planet_ok = (planet_data and planet_data.get("status") == "ok"
                     and planet_data.get("scene_count", 0) > 0)
        if parcel_id and county and not skip_planet and planet_ok:
            try:
                conn = get_db_connection()
                update_parcel_planet(conn, parcel_id, county, planet_data)
                conn.close()
            except Exception as e:
                logger.warning("planet_persist_failed", error=str(e))

        if skip_planet and planet_skip_reason:
            result["_planet_skipped"] = planet_skip_reason

        return result
    except Exception as e:
        logger.error("scan_endpoint_error", lat=lat, lng=lng, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scan_free")
def api_scan_free(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
):
    """Free scan: NAIP + FEMA only (batch mode)."""
    try:
        result = scan_free(lat, lng)
        return result
    except Exception as e:
        logger.error("scan_free_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/naip_baseline")
def api_naip_baseline(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
):
    """NAIP-only baseline check."""
    try:
        return naip_baseline(lat, lng)
    except Exception as e:
        logger.error("naip_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fema_flood")
def api_fema_flood(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
):
    """FEMA flood zone lookup."""
    try:
        return fema_flood(lat, lng)
    except Exception as e:
        logger.error("fema_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/planet_search")
def api_planet_search(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    months: int = Query(18, ge=1, le=36),
):
    """Search Planet scene archive at a point."""
    if not PLANET_ENABLED:
        return {"status": "disabled", "message": "Planet is feature-flagged off. Set PLANET_ENABLED=true to enable."}
    try:
        return planet_search(lat, lng, months_back=months)
    except Exception as e:
        logger.error("planet_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/landsat_trends")
def api_landsat_trends(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    months: int = Query(24, ge=1, le=48),
):
    """Landsat NDVI trends (Sentinel backup)."""
    try:
        return landsat_trends(lat, lng, months=months)
    except Exception as e:
        logger.error("landsat_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/enrich_sentinel")
def api_enrich_sentinel(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    parcel_id: str = Query(None, description="Parcel ID for DB persistence"),
    county: str = Query(None, description="County name for DB persistence"),
    state: str = Query("NC", description="State code"),
    months: int = Query(12, ge=1, le=24),
):
    """Sentinel-2 enrichment with optional DB persistence."""
    try:
        result = enrich_sentinel(lat, lng, months=months, include_rgb=True)

        # Persist to DB if parcel_id and county provided
        if parcel_id and county:
            try:
                conn = get_db_connection()
                # Build rescored result using existing NAIP/FEMA from DB
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT ndvi_score, fema_zone, fema_risk, fema_sfha
                        FROM gis_parcels_core
                        WHERE parcel_id = %s AND county = %s
                    """, (parcel_id, county))
                    row = cur.fetchone()

                if row:
                    fema_data = None
                    if row.get("fema_zone"):
                        fema_data = {
                            "fema_zone": row["fema_zone"],
                            "fema_risk": row.get("fema_risk"),
                            "fema_sfha": row.get("fema_sfha"),
                        }
                    rescore = rescore_with_sentinel(
                        naip_ndvi=row.get("ndvi_score"),
                        fema_data=fema_data,
                        sentinel_result=result,
                    )

                    db_row = {
                        "parcel_id": parcel_id,
                        "county": county,
                        **{k: v for k, v in result.items()
                           if not k.startswith("_") and k != "errors"},
                        **rescore,
                    }
                    batch_update_sentinel_results(conn, [db_row])
                    result["db_write"] = {"status": "ok", "parcel_id": parcel_id}
                else:
                    result["db_write"] = {"status": "parcel_not_found"}

                conn.close()
            except Exception as e:
                logger.error("enrich_sentinel_db_error", error=str(e))
                result["db_write"] = {"status": "error", "detail": str(e)}

        return result
    except Exception as e:
        logger.error("enrich_sentinel_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/check_usps_vacancy")
def api_check_usps_vacancy(
    street: str = Query(..., description="Street address (e.g. '123 MAIN ST')"),
    city: str = Query(None, description="City name"),
    state: str = Query(None, description="State code (e.g. NC)"),
    zip_code: str = Query(None, description="ZIP code"),
    parcel_id: str = Query(None, description="Parcel ID for DB persistence"),
    county: str = Query(None, description="County name for DB persistence"),
    account: int = Query(1, ge=1, le=9, description="USPS account number"),
    force: bool = Query(False, description="Bypass 60-day cache"),
):
    """Check a single address for USPS vacancy status."""
    try:
        # Cache check (only if parcel_id + county provided)
        if parcel_id and county and not force:
            conn = get_db_connection()
            cached = get_usps_cache(conn, parcel_id, county, cache_days=60)
            conn.close()
            if cached:
                return {
                    "source": "cache",
                    "cached_at": cached["checked_at"].isoformat() if cached.get("checked_at") else None,
                    "vacant": cached.get("vacant"),
                    "dpv_confirmed": cached.get("dpv_confirmed"),
                    "usps_address": cached.get("usps_address"),
                    "usps_city": cached.get("usps_city"),
                    "usps_zip": cached.get("usps_zip"),
                    "usps_zip4": cached.get("usps_zip4"),
                    "address_mismatch": cached.get("address_mismatch"),
                    "carrier_route": cached.get("carrier_route"),
                    "error": cached.get("error"),
                }

        # Resolve lat/lng for Nominatim disambiguation if parcel_id + county available
        plat, plng = None, None
        if parcel_id and county and (not city and not zip_code):
            try:
                from psycopg2.extras import RealDictCursor
                conn = get_db_connection()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""SELECT latitude, longitude FROM gis_parcels_core
                                   WHERE parcel_id = %s AND county = %s""",
                                (parcel_id, county))
                    prow = cur.fetchone()
                conn.close()
                if prow:
                    plat = prow.get("latitude")
                    plng = prow.get("longitude")
            except Exception:
                pass

        # Call USPS API (auto-resolves city/zip via Nominatim if missing)
        checker = USPSVacancyChecker(account=account)
        result = checker.check_address(street, city=city, state=state, zip_code=zip_code,
                                       county=county, lat=plat, lng=plng)

        # Evaluate flag
        usps_data = {
            "vacant": result.vacant,
            "dpv_confirmed": result.dpv_confirmed,
            "address_mismatch": result.address_mismatch,
            "usps_address": result.usps_address,
            "usps_city": result.usps_city,
            "usps_zip": result.usps_zip,
            "carrier_route": result.carrier_route,
        }
        flag_result = evaluate_usps_vacancy(usps_data)

        response = {
            "source": "api",
            "vacant": result.vacant,
            "dpv_confirmed": result.dpv_confirmed,
            "business": result.business,
            "carrier_route": result.carrier_route,
            "usps_address": result.usps_address,
            "usps_city": result.usps_city,
            "usps_state": result.usps_state,
            "usps_zip": result.usps_zip,
            "usps_zip4": result.usps_zip4,
            "address_mismatch": result.address_mismatch,
            "error": result.error,
            "flag": flag_result,
        }

        # Persist to DB if parcel_id + county provided
        if parcel_id and county:
            try:
                conn = get_db_connection()
                # Write to gis_parcels_core (always works, standalone-safe)
                update_parcel_usps(conn, parcel_id, county, result,
                                  flag_vacancy=flag_result["flag"],
                                  vacancy_confidence=flag_result["confidence"])
                # Write to shared audit table (best-effort, needs parcels/counties)
                save_usps_check(conn, parcel_id, county, state or "NC",
                                result, account)
                conn.close()
                response["db_write"] = {"status": "ok", "parcel_id": parcel_id}
            except Exception as e:
                logger.error("usps_db_error", error=str(e))
                response["db_write"] = {"status": "error", "detail": str(e)}

        return response
    except Exception as e:
        logger.error("usps_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/parcels")
def api_parcels(
    county: str = Query(..., description="County name"),
    state: str = Query("NC", description="State code"),
    property_class: str = Query(None),
    min_value: float = Query(None),
    max_value: float = Query(None),
    min_sqft: float = Query(None),
    max_sqft: float = Query(None),
    zip: str = Query(None, description="Mailing zip prefix"),
    min_score: float = Query(None, description="Min distress score"),
    max_score: float = Query(None, description="Max distress score"),
    flag: str = Query(None, description="Comma-separated flags: veg,flood,structural,neglect"),
    fema_zone: str = Query(None, description="FEMA zone filter"),
    scanned_only: bool = Query(False, description="Only return scanned parcels"),
    min_composite: float = Query(None, description="Min distress composite score"),
    min_conviction: float = Query(None, description="Min conviction score"),
    sort_by: str = Query("parcel_id", description="Sort: parcel_id, distress_score, distress_composite, ndvi_slope, conviction_score"),
    limit: int = Query(500, le=5000),
    offset: int = Query(0),
):
    """Load filtered parcels with property details + scan data from GIS."""
    try:
        from psycopg2.extras import RealDictCursor
        conn = get_db_connection()

        # Base columns — property + scan data
        select_cols = """
            parcel_id, pin, owner_name, owner_name2,
            mailing_address1, mailing_city, mailing_state, mailing_zip,
            situs_address, property_class,
            land_value, improvement_value, total_value,
            sale_date, sale_amount, deed_book, deed_page,
            latitude, longitude,
            tax_year, tax_total,
            bedrooms, bathrooms, sqft, year_built,
            county, state_code,
            ndvi_score, ndvi_date, ndvi_category,
            fema_zone, fema_risk, fema_sfha,
            distress_score, distress_flags,
            flag_veg, flag_flood, flag_structural, flag_neglect,
            veg_confidence, flood_confidence,
            scan_date, scan_pass, sentinel_worthy,
            sentinel_trend_direction, sentinel_trend_slope,
            sentinel_latest_ndvi, sentinel_months_data,
            sentinel_mean_ndvi, sentinel_data_source,
            sentinel_chart_url, sentinel_scan_date,
            planet_scan_date,
            planet_scene_count, planet_change_score, planet_temporal_span,
            planet_latest_date, planet_earliest_date,
            planet_thumb_latest_url, planet_thumb_earliest_url,
            ndvi_slope_5yr, ndvi_slope_pctile, ndvi_history_count,
            ndvi_history_years, distress_composite, composite_date,
            usps_vacant, usps_dpv_confirmed, usps_address, usps_city,
            usps_zip, usps_zip4, usps_check_date,
            flag_vacancy, vacancy_confidence,
            conviction_score, conviction_base_score, conviction_vacancy_bonus,
            conviction_mc_score, conviction_mc_signals, conviction_mc_codes,
            conviction_components, conviction_date
        """

        where = "WHERE county = %s AND state_code = %s AND latitude IS NOT NULL AND longitude IS NOT NULL"
        params = [county, state]

        if property_class:
            where += " AND property_class = %s"
            params.append(property_class)
        if min_value is not None:
            where += " AND total_value >= %s"
            params.append(min_value)
        if max_value is not None:
            where += " AND total_value <= %s"
            params.append(max_value)
        if min_sqft is not None:
            where += " AND sqft >= %s"
            params.append(min_sqft)
        if max_sqft is not None:
            where += " AND sqft <= %s"
            params.append(max_sqft)
        if zip:
            where += " AND SUBSTRING(mailing_zip FROM 1 FOR 5) = %s"
            params.append(zip[:5])
        if min_score is not None:
            where += " AND distress_score >= %s"
            params.append(min_score)
        if max_score is not None:
            where += " AND distress_score <= %s"
            params.append(max_score)
        if scanned_only:
            where += " AND scan_date IS NOT NULL"
        if fema_zone:
            where += " AND fema_zone = %s"
            params.append(fema_zone)
        if flag:
            flag_map = {"veg": "flag_veg", "flood": "flag_flood",
                        "structural": "flag_structural", "neglect": "flag_neglect",
                        "vacancy": "flag_vacancy"}
            flag_cols = [flag_map[f.strip()] for f in flag.split(",")
                         if f.strip() in flag_map]
            if flag_cols:
                or_clause = " OR ".join(f"{col} = TRUE" for col in flag_cols)
                where += f" AND ({or_clause})"

        if min_composite is not None:
            where += " AND distress_composite >= %s"
            params.append(min_composite)
        if min_conviction is not None:
            where += " AND conviction_score >= %s"
            params.append(min_conviction)

        # Sort order
        sort_map = {
            "parcel_id": "parcel_id",
            "distress_score": "distress_score DESC NULLS LAST",
            "distress_composite": "distress_composite DESC NULLS LAST",
            "ndvi_slope": "ndvi_slope_5yr DESC NULLS LAST",
            "conviction_score": "conviction_score DESC NULLS LAST",
        }
        order_by = sort_map.get(sort_by, "parcel_id")

        # Count query
        count_params = list(params)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM gis_parcels_core {where}", count_params)
            total = cur.fetchone()[0]

        # Data query
        query = f"SELECT {select_cols} FROM gis_parcels_core {where} ORDER BY {order_by} LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = [dict(r) for r in cur.fetchall()]

        conn.close()

        # Post-process: serialize types + build nested objects for dashboard
        for row in rows:
            # Serialize dates/decimals
            for k, v in list(row.items()):
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()
                elif hasattr(v, 'is_finite'):  # Decimal
                    row[k] = float(v) if v is not None else None

            # Build nested flag objects (dashboard expects flags[] array)
            flags = []
            if row.get("flag_veg"):
                flags.append({"signal_code": "vegetation_overgrowth",
                              "confidence": row.get("veg_confidence")})
            if row.get("flag_neglect"):
                flags.append({"signal_code": "vegetation_neglect",
                              "confidence": row.get("veg_confidence")})
            if row.get("flag_flood"):
                flags.append({"signal_code": "flood_risk",
                              "confidence": row.get("flood_confidence")})
            if row.get("flag_structural"):
                flags.append({"signal_code": "structural_change",
                              "confidence": None})
            if row.get("flag_vacancy"):
                flags.append({"signal_code": "usps_vacancy",
                              "confidence": row.get("vacancy_confidence")})
            row["flags"] = flags

            # Build nested fema object
            if row.get("fema_zone"):
                row["fema"] = {
                    "flood_zone": row["fema_zone"],
                    "risk_level": row.get("fema_risk"),
                    "is_sfha": row.get("fema_sfha", False),
                }
            else:
                row["fema"] = None

            # Build nested naip object
            if row.get("ndvi_score") is not None:
                row["naip"] = {
                    "current_ndvi": row["ndvi_score"],
                    "ndvi_date": row.get("ndvi_date"),
                    "category": row.get("ndvi_category"),
                    "slope_5yr": row.get("ndvi_slope_5yr"),
                    "slope_pctile": row.get("ndvi_slope_pctile"),
                    "history_count": row.get("ndvi_history_count"),
                    "history_years": row.get("ndvi_history_years"),
                }
            else:
                row["naip"] = None

            # Build nested composite object
            if row.get("distress_composite") is not None:
                row["composite"] = {
                    "score": row["distress_composite"],
                    "ndvi_slope_pctile": row.get("ndvi_slope_pctile"),
                    "computed_at": row.get("composite_date"),
                }
            else:
                row["composite"] = None

            # Build nested sentinel object
            if row.get("sentinel_months_data") and row["sentinel_months_data"] > 0:
                row["sentinel"] = {
                    "trend_direction": row.get("sentinel_trend_direction"),
                    "trend_slope": row.get("sentinel_trend_slope"),
                    "latest_ndvi": row.get("sentinel_latest_ndvi"),
                    "months_with_data": row["sentinel_months_data"],
                    "mean_ndvi": row.get("sentinel_mean_ndvi"),
                    "data_source": row.get("sentinel_data_source"),
                    "chart_url": row.get("sentinel_chart_url"),
                    "scan_date": row.get("sentinel_scan_date"),
                    "monthly_ndvi": [],
                }
            else:
                row["sentinel"] = None

            # Build nested planet object
            if row.get("planet_scene_count") and row["planet_scene_count"] > 0:
                planet_dr = {}
                if row.get("planet_earliest_date"):
                    planet_dr["earliest"] = row["planet_earliest_date"]
                if row.get("planet_latest_date"):
                    planet_dr["latest"] = row["planet_latest_date"]
                row["planet"] = {
                    "scene_count": row["planet_scene_count"],
                    "change_score": row.get("planet_change_score"),
                    "temporal_span_days": row.get("planet_temporal_span"),
                    "date_range": planet_dr if planet_dr else None,
                    "thumbnail_url": row.get("planet_thumb_latest_url"),
                    "thumbnail_latest_url": row.get("planet_thumb_latest_url"),
                    "thumbnail_earliest_url": row.get("planet_thumb_earliest_url"),
                }
            else:
                row["planet"] = None

            # Build nested usps object
            if row.get("usps_check_date") is not None:
                row["usps"] = {
                    "vacant": row.get("usps_vacant"),
                    "dpv_confirmed": row.get("usps_dpv_confirmed"),
                    "usps_address": row.get("usps_address"),
                    "usps_city": row.get("usps_city"),
                    "usps_zip": row.get("usps_zip"),
                    "usps_zip4": row.get("usps_zip4"),
                    "check_date": row.get("usps_check_date"),
                }
            else:
                row["usps"] = None

            # Build nested conviction object
            if row.get("conviction_score") is not None:
                mc_codes_raw = row.get("conviction_mc_codes") or ""
                row["conviction"] = {
                    "score": row["conviction_score"],
                    "base_score": row.get("conviction_base_score"),
                    "vacancy_bonus": row.get("conviction_vacancy_bonus"),
                    "components": (row.get("conviction_components") or "").split(",") if row.get("conviction_components") else [],
                    "mc_score": row.get("conviction_mc_score"),
                    "mc_signals": row.get("conviction_mc_signals"),
                    "mc_codes": mc_codes_raw.split(",") if mc_codes_raw else [],
                    "computed_at": row.get("conviction_date"),
                }
            else:
                row["conviction"] = None

        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "count": len(rows),
            "parcels": rows,
        }
    except Exception as e:
        logger.error("parcels_endpoint_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
