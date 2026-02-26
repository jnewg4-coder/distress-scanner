# Distress Scanner — LLM Knowledge Bank

Reference doc for future LLM sessions working on this codebase. Covers architecture, data sources, thresholds, DB schema, API, and operations.

## Architecture Overview

**DistressScannerApp** detects property distress signals from satellite/aerial imagery, public flood data, USPS vacancy flags, and motivation-curator signals. Five-pass pipeline:

```
225K+ raw parcels (gis_parcels_core)
  → Pass 1: NAIP NDVI + FEMA (free, unlimited, ~9.4 parcels/sec)
     72,714 Gaston residential scanned, 22.3% flagged, 0 errors
  → Pass 1.5: NDVI historical slope (Planetary Computer COG reads, ~2.2/sec)
     72,708 slopes computed, composite scores written
  → Pass 1.5b: Sentinel-2 NDVI trends (free, 1 CDSE req/parcel, ~30/min)
  → Pass 2: USPS vacancy enrichment (~60/hr per API key)
     1,357 checked (99.4% of composite>=7.5), 46 vacant, 31 errors
  → Pass 2.5: Conviction Score Fusion (DS + MC + USPS → unified score)
     77,869 scored, 10,183 MC-joined
  → Pass 3: Planet 3m imagery (paid, 30K trial budget)
```

**scan_pass semantics**: 1=NAIP+FEMA, 2=Sentinel/Landsat enriched, 3=Planet refined. Monotonic: `GREATEST(COALESCE(scan_pass,0), new_pass)` prevents downgrades.

**Tech stack**: Python 3.11, FastAPI, psycopg2, requests, structlog. PostgreSQL on Railway. R2 object storage for images/charts. Single-page dashboard (vanilla HTML/JS, Leaflet, Chart.js).

**Design principle**: Standalone-first. Build features to run independently of motivation-curator. Cache checks use `gis_parcels_core` directly (parcel_id + county), not UUID JOINs. Shared audit tables (e.g. `usps_vacancy_checks`) are best-effort writes.

## Key File Paths

| Path | Purpose |
|------|---------|
| `src/api/app.py` | FastAPI server, endpoints, dashboard serving (port 8001) |
| `src/dashboard/index.html` | Command Center SPA — filters, table, map, detail panel |
| `src/analysis/flags.py` | Distress flag evaluators (veg overgrowth, neglect, flood, structural, vacancy) |
| `src/analysis/scanner.py` | Scan orchestrators: `scan_free()`, `scan_distress()`, `enrich_sentinel()`, `rescore_with_sentinel()` |
| `src/naip/client.py` | NAIP ArcGIS REST client — identify, export, NDVI compute |
| `src/naip/baseline.py` | `naip_baseline()` (full), `naip_ndvi_fast()` (batch — single call, no history) |
| `src/fema/client.py` | FEMA NFHL ArcGIS client — flood zone query + map tiles |
| `src/fema/flood.py` | `fema_flood()` — high-level flood analysis |
| `src/sentinel/client.py` | SentinelHub CDSE client — statistical API, evalscripts |
| `src/sentinel/trends.py` | `sentinel_trends()` — monthly NDVI trends, slope, chart generation |
| `src/landsat/client.py` | Landsat NDVI via Esri Living Atlas (`landsat2.arcgis.com`) — Sentinel fallback |
| `src/planet/client.py` | Planet Labs Data API v1 — scene search, thumbnails, refine |
| `src/usps/vacancy.py` | USPS Address REST API v3 — vacancy checks, `split_situs()`, rate limiting |
| `src/db.py` | DB connection, migrations (6 idempotent), batch update, parcel queries, cache helpers |
| `src/signals.py` | Signal type definitions, `write_scan_results()` |
| `src/storage.py` | R2 upload helpers |
| `scripts/batch_ndvi_scan.py` | **Pass 1 batch** — ThreadPoolExecutor(10), ~9.4 parcels/sec |
| `scripts/batch_historical_slope.py` | **Pass 1.5 batch** — Planetary Computer NAIP historical + composite scoring |
| `scripts/batch_sentinel_enrich.py` | **Pass 1.5b batch** — Sentinel enrichment, adaptive throttling |
| `scripts/batch_usps_enrich.py` | **Pass 2 batch** — USPS vacancy enrichment, mailing address fallback |
| `scripts/batch_conviction_score.py` | **Pass 2.5 batch** — Conviction Score Fusion (DS+MC+USPS) |
| `scripts/test_usps_single.py` | Single-address USPS vacancy test |
| `scripts/register_signal_types.py` | Register signal types in DB (one-time) |

## Data Sources

### NAIP (National Agriculture Imagery Program)
- **API**: USGS ArcGIS ImageServer REST (`https://imagery.nationalmap.gov/arcgis/rest/services/USGSNAIPPlus/ImageServer`)
- **Resolution**: 1m aerial (RGB + NIR)
- **Cost**: Free, unlimited, no API key. Use USGS endpoint ONLY — Esri endpoint DNS fails.
- **Coverage**: US only, 2-3 year cycle per state. Not annual.
- **Key ops**: identify (point → band values), exportImage (bbox → PNG)
- **NDVI**: Computed from bands: `(NIR - Red) / (NIR + Red)`
- **Dates**: Field is lowercase `acquisition_date` (epoch ms), filter `Category == 1`
- **Cache**: 7-day file cache in `data/cache/naip/`, SHA256 keyed
- **Batch**: `naip_ndvi_fast()` — single API call, no history, module-level shared `requests.Session` (thread-safe for GETs)

### FEMA NFHL (National Flood Hazard Layer)
- **API**: ArcGIS MapServer REST (`https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer`)
- **Layer**: 28
- **Cost**: Free, unlimited
- **Fields**: `FLD_ZONE, SFHA_TF, ZONE_SUBTY, FLD_AR_ID, STATIC_BFE` — `FLOODWAY` field does NOT exist
- **Zone X**: Check `ZONE_SUBTY` content — "MINIMAL" = low risk, "500" = moderate risk
- **High-risk zones**: A, AE, AO, VE, V (Special Flood Hazard Area)

### Sentinel-2 (CDSE)
- **API**: Copernicus Data Space Ecosystem via sentinelhub-py
- **Resolution**: 10m, 5-day revisit
- **Cost**: Free tier — 10,000 requests/month, 300 req/min
- **Usage**: Pass 1.5 enrichment. Stats-only mode = 1 CDSE request/parcel.
- **Key fix**: Statistical API MUST use `size=(50,50)` not `resolution=(10,10)` — resolution returns all zeros on CDSE
- **Evalscript**: Must include `dataMask` in both input bands and output

### Landsat (USGS/Esri Living Atlas)
- **API**: `landsat2.arcgis.com` — Esri Living Atlas service, same REST pattern as NAIP
- **Resolution**: 30m
- **Cost**: Free, zero auth
- **Usage**: Sentinel fallback for NDVI trends when CDSE has no data

### Planet Labs
- **API**: Data API **v1** (NOT v2 — v2 never existed). `https://api.planet.com/data/v1/`
- **Resolution**: 3-5m daily (PlanetScope)
- **Cost**: Paid — 30K request trial. API key in `Planet_API` env var.
- **Auth**: `api-key {PLAK_...}` header
- **Tiles API**: `tiles.planet.com/data/v1/`
- **Search**: `quick-search` returns most-recent-first; for temporal pairs use two targeted date-range searches
- **Re-run guard**: 60-day cooldown via `planet_scan_date` column. `force_planet=true` overrides.

### USPS Address API v3
- **API**: `https://apis.usps.com/addresses/v3/address` — REST, NOT RETS
- **Auth**: OAuth2 client_credentials → Bearer token
- **Rate limit**: 60 requests/hour per Consumer Key (token-based, NOT IP-based)
- **Delays**: Env-configurable `USPS_CHAIN_DELAY_MIN/MAX` (default 30-55s). Random jitter, NOT fixed intervals.
- **Why jitter matters**: Bot detection patterns (fixed intervals = flagged) + SpikeArrest micro-window avoidance
- **429 backoff**: Exponential starting 120s, doubling each consecutive 429, cap 900s. Respects Retry-After header.
- **Multi-account**: `USPS_CLIENT_ID/_SECRET` for account 1, `_2`/`_3` suffixes for additional accounts
- **Cache**: 60-day window via `usps_check_date` on `gis_parcels_core` (standalone, no UUID dependency)
- **Shared audit**: Best-effort write to `usps_vacancy_checks` table (shared with motivation-curator)
- **Test env**: `apis-tem.usps.com` available (same creds, doesn't burn prod quota)

## NDVI Thresholds (RE Distress Tuned)

| NDVI Range | Category | Interpretation | Flag |
|------------|----------|----------------|------|
| < 0.10 | bare | Impervious surface, rock | — |
| 0.10 – 0.30 | minimal/neglect | Abandoned lot, severe neglect | `vegetation_neglect` |
| 0.30 – 0.50 | sparse | Maintained lawn, watch zone | — |
| 0.50 – 0.65 | moderate | Dense vegetation, moderate overgrowth | `vegetation_overgrowth` (with history) |
| > 0.65 | dense | Very dense, strong overgrowth | `vegetation_overgrowth` (high confidence) |

### Flag evaluation logic:
- **vegetation_overgrowth**: Two-tier. Strong (>0.65) flags even without history at 0.6 conf. Moderate (0.50-0.65) only flags with historical delta >0.15. NAIP + Sentinel agreement boosts confidence.
- **vegetation_neglect**: NDVI 0.10-0.30. Confidence inversely proportional to NDVI. Boosted +0.15 if in flood zone.
- **flood_risk**: Zone-based. High-risk (A/AE/VE) = 1.0 conf, moderate = 0.6.
- **structural_change**: NDVI drop >0.20 from historical baseline. NAIP + Sentinel agreement boosts.
- **usps_vacancy**: Carrier-confirmed vacant (90+ days no mail). Confidence: 0.90 (vacant+DPV confirmed), 0.75 (vacant, DPV unknown), capped 0.70 if address mismatch.

### Distress Score
Weighted sum of flag confidences, capped at 10.0:
```
vegetation_overgrowth: 2.0 × confidence
vegetation_neglect:    1.5 × confidence
flood_risk:            1.5 × confidence
structural_change:     2.5 × confidence
usps_vacancy:          2.5 × confidence
```

### Composite Score (Phase 1)
Bulk distress scoring from NAIP historical slope + FEMA:
```
composite = 0.70 × NDVI_SLOPE_PCTILE + 0.30 × FEMA_RISK
```
- NDVI slope: linear regression on multi-year NAIP vintages (2012-2024 via Planetary Computer)
- `compute_ndvi_slope()` in baseline.py — manual least-squares, no numpy
- `compute_composite_scores()` in db.py — SQL `PERCENT_RANK()` + weighted formula
- DB columns: `ndvi_slope_5yr`, `ndvi_slope_pctile`, `ndvi_history_count`, `distress_composite`
- Distribution (Gaston): 9% HIGH (>=7), 28.5% MODERATE (5-6.9), 28.4% LOW (3-4.9), 34% MINIMAL (<3)

### Conviction Score Fusion (Phase 2.5)
Unified score combining DS (distress_composite) + MC (motivation-curator signals) + USPS vacancy:
```
Formula: reweighted average (missing components excluded, not zero)
  W_DS = 0.35, W_MC = 0.40, MC_CAP = 7.0, VAC_BONUS_MAX = 2.5

  ds_comp = clamp(distress_composite / 10.0, 0, 1)  -- None if no composite
  mc_comp = clamp(mc_raw / MC_CAP, 0, 1)             -- None if mc_count == 0
  base_sum = (W_DS if ds_comp exists) + (W_MC if mc_comp exists)
  base = 10 × ((W_DS × ds_comp) + (W_MC × mc_comp)) / base_sum
  vacancy_bonus = VAC_BONUS_MAX × vacancy_confidence  -- only when USPS confirmed vacant
  conviction = clamp(base + vacancy_bonus, 0, 10)
```
- **Canonical JOIN**: Must use `counties.name + state_code` compound key to avoid cross-county signal leaks (1,870 duplicate parcel_ids across counties)
- **MC signals**: From `parcel_signals` table (motivation-curator). Codes: `absentee_owner`, `high_equity`, `tax_delinquent`, `pre_foreclosure`, `code_violation`, etc.
- **Acceptance checks**: parcel 312390=7.59 (DS-only), parcel 107838=8.83 (DS+MC+VAC)
- **DB columns**: `conviction_score`, `conviction_base_score`, `conviction_vacancy_bonus`, `conviction_mc_score`, `conviction_mc_signals`, `conviction_mc_codes`, `conviction_components`, `conviction_date`
- **Index**: `idx_gpc_conviction_score` (DESC NULLS LAST)
- **motivation_scores backfill**: County-scoped DELETE + INSERT (unique constraint is `(parcel_id, computed_at)`)
- Batch script: `scripts/batch_conviction_score.py` — 225,861 parcels, 77,869 scored, 10,183 MC joined, 208.9s

## Database Schema

### gis_parcels_core (main table)
**Property columns** (pre-existing):
`parcel_id`, `pin`, `owner_name`, `owner_name2`, `mailing_address1`, `mailing_city`, `mailing_state`, `mailing_zip`, `situs_address`, `property_class`, `land_value`, `improvement_value`, `total_value`, `sale_date`, `sale_amount`, `deed_book`, `deed_page`, `latitude`, `longitude`, `tax_year`, `tax_total`, `bedrooms`, `bathrooms`, `sqft`, `year_built`, `county`, `state_code`

**Scan columns** (added by `migrate_add_scan_columns`):
`ndvi_score`, `ndvi_date`, `ndvi_category`, `fema_zone`, `fema_risk`, `fema_sfha`, `distress_score`, `distress_flags`, `flag_veg`, `flag_flood`, `flag_structural`, `flag_neglect`, `veg_confidence`, `flood_confidence`, `scan_date`, `scan_pass`, `sentinel_worthy`, `planet_scan_date`

**Sentinel columns** (added by `migrate_add_sentinel_columns`):
`sentinel_trend_direction`, `sentinel_trend_slope`, `sentinel_latest_ndvi`, `sentinel_months_data`, `sentinel_mean_ndvi`, `sentinel_data_source`, `sentinel_chart_url`, `sentinel_scan_date`

**USPS columns** (added by `migrate_add_usps_columns`):
`usps_vacant`, `usps_dpv_confirmed`, `usps_address`, `usps_city`, `usps_zip`, `usps_zip4`, `usps_business`, `usps_carrier_route`, `usps_address_mismatch`, `usps_check_date`, `usps_error`, `flag_vacancy`, `vacancy_confidence`

**Planet columns** (added by `migrate_add_planet_columns`):
`planet_scene_count`, `planet_change_score`, `planet_temporal_span`, `planet_latest_date`, `planet_earliest_date`, `planet_thumb_latest_url`, `planet_thumb_earliest_url`

**Composite columns** (added by `migrate_add_composite_columns`):
`ndvi_slope_5yr`, `ndvi_slope_pctile`, `ndvi_history_count`, `ndvi_history_years`, `distress_composite`, `composite_date`

**Conviction columns** (added by `migrate_add_conviction_columns`):
`conviction_score`, `conviction_base_score`, `conviction_vacancy_bonus`, `conviction_mc_score`, `conviction_mc_signals`, `conviction_mc_codes`, `conviction_components`, `conviction_date`

**Key indexes**: `ndvi_score`, `distress_score`, `fema_zone`, `flag_veg`, `flag_flood`, `flag_neglect`, `scan_date`, `sentinel_scan_date`, `sentinel_trend_direction`, `usps_vacant`, `flag_vacancy`, `usps_check_date`, `idx_gpc_conviction_score` (DESC NULLS LAST)

### usps_vacancy_checks (shared audit table)
Matches motivation-curator schema. Uses `parcel_id UUID REFERENCES parcels(id)`.
Fields: `id`, `parcel_id`, `checked_at`, `input_address`, `input_state`, `usps_address`, `usps_city`, `usps_state`, `usps_zip`, `usps_zip4`, `vacant`, `dpv_confirmed`, `business`, `address_mismatch`, `carrier_route`, `account`, `error`, `raw_response`

### signal_types
`id` (UUID), `code`, `name`, `description`, `base_weight`, `decay_type`, `decay_days`

### parcel_signals
`id` (UUID), `parcel_id` (→ parcels), `signal_type_id` (→ signal_types), `signal_date`, `confidence`, `evidence` (JSONB), `is_active`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Service health check |
| `/dashboard` | GET | Serve Command Center HTML |
| `/parcels` | GET | Filtered parcels + scan data. Params: `county`, `state`, `property_class`, `min_value`, `max_value`, `min_sqft`, `max_sqft`, `zip`, `min_score`, `max_score`, `min_conviction`, `flag` (veg,flood,structural,neglect,vacancy), `fema_zone`, `scanned_only`, `sort_by` (distress_composite, conviction_score), `limit`, `offset`. Returns nested objects: `flags[]`, `fema{}`, `naip{}`, `sentinel{}`, `usps{}`, `conviction{}` |
| `/scan_distress` | GET | Full scan (NAIP+Sentinel+FEMA+Planet). Params: `lat`, `lng`, `months`, `parcel_id`, `county`, `force_planet`. Planet re-run guard: skips if scanned within 60 days unless `force_planet=true`. |
| `/scan_free` | GET | Free scan (NAIP+FEMA only). Params: `lat`, `lng` |
| `/enrich_sentinel` | GET | Sentinel-2 enrichment with DB persistence. Params: `lat`, `lng`, `parcel_id`, `county`, `state`, `months` |
| `/check_usps_vacancy` | GET | USPS vacancy check. Params: `street`, `city`, `state`, `zip_code`, `parcel_id`, `county`, `account`, `force`. 60-day cache via `gis_parcels_core`. |
| `/naip_baseline` | GET | NAIP-only baseline. Params: `lat`, `lng` |
| `/fema_flood` | GET | FEMA flood lookup. Params: `lat`, `lng` |
| `/planet_search` | GET | Planet scene search. Params: `lat`, `lng`, `months` |
| `/landsat_trends` | GET | Landsat NDVI trends. Params: `lat`, `lng`, `months` |

## Dashboard Features

- **Filter pills**: VEG, NEG, FLD, STR, VAC (flag toggles). FEMA zone pills. Property class multi-select.
- **Show modes**: ALL, SCANNED, HOT, SKIP, NEW, SENT (sentinel-enriched).
- **Score range**: Min/max distress score inputs. Min conviction score input.
- **Presets**: GASTON 28052, GASTON SCANNED, TOP LEADS (composite), CONVICTION (conviction_score sorted, server-side `min_conviction`), MECKLENBURG SFR.
- **Scan badge**: 3-tier — NAIP+FEMA (pass 1) / NAIP+FEMA+SENT (pass 2) / PLANET 3m (pass 3).
- **Absentee filter**: Heuristic comparing situs vs mailing address.
- **Table**: Sortable columns incl Score, Comp, Conv (conviction). Keyboard nav (j/k/h/s), hot/skip marks.
- **Map**: Leaflet with color-coded markers by distress score.
- **Detail panel**: Property info, NAIP aerial + NDVI images, NDVI slope/composite cards, **Conviction Score card** (stacked bar showing actual DS/MC/VAC contributions computed from formula constants), **MC signal pills** (ABSENTEE, HIGH EQ, TAX DEL, etc.), Sentinel trend chart, USPS vacancy status, scan data, flags with confidence bars.
- **Conviction breakdown**: Reverse-engineers DS/MC split from `W_DS=0.35, W_MC=0.40, MC_CAP=7.0` constants and parcel-level data. Labels show actual point contributions, not hardcoded ratios.
- **Street View**: Google Maps JS API with `StreetViewService` — finds nearest panorama, computes heading from camera to property via `geometry.spherical.computeHeading()`. Requires `GOOGLE_MAPS_BROWSER_KEY` env var. Falls back to clickable link if no key. Cost: $14/1K loads, $200/mo free credit (~14K free loads/mo).
- **Satellite embed**: `maps.google.com/maps?q=ADDRESS&t=k&z=19&output=embed` — free, no key needed.
- **Map markers**: Color by distress score — red (>=3), orange (>=1.5), amber/yellow (>0), blue (unscanned).
- **Metrics bar**: Parcels, Avg Value, Avg SqFt, Flagged, Avg Score, Avg Comp, **Avg Conv**, Hot, Showing.
- **SENT button**: Triggers `/enrich_sentinel` for individual parcels.
- **Assessment narrative**: Auto-generated from NAIP, Sentinel trends, FEMA, Planet, USPS vacancy, and conviction data. Conviction >=8 triggers INVESTIGATE. MC signal count shown as tags. Recommendations are flag-specific (not generic).
- **Export**: CSV (filtered) with 40+ columns incl conviction_score/base/bonus/components, mc_motivation_score/signal_count/codes, sentinel_trend_direction/slope/latest_ndvi, usps_vacant/address/city/zip, ndvi_slope_5yr, distress_composite. JSON (hot parcels).

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string (Railway) |
| `SH_CLIENT_ID` | For Sentinel | Sentinel Hub CDSE OAuth client ID |
| `SH_CLIENT_SECRET` | For Sentinel | Sentinel Hub CDSE OAuth client secret |
| `Planet_API` | For Planet | Planet Labs API key (PLAK format) |
| `USPS_CLIENT_ID` | For USPS | USPS developer account 1 client ID |
| `USPS_CLIENT_SECRET` | For USPS | USPS developer account 1 secret |
| `USPS_CLIENT_ID_2` | Optional | Account 2 for parallel USPS runs |
| `USPS_CLIENT_SECRET_2` | Optional | Account 2 secret |
| `USPS_CHAIN_DELAY_MIN` | Optional | Min random delay between USPS calls (default 30s) |
| `USPS_CHAIN_DELAY_MAX` | Optional | Max random delay between USPS calls (default 55s) |
| `GOOGLE_MAPS_BROWSER_KEY` | For Street View | Browser-only Google Maps key (Maps JS API + geometry). Restrict in GCP: HTTP referrer + Maps JS API only. Never use for server-side calls. |
| `R2_ENDPOINT` | Optional | Cloudflare R2 endpoint for image storage |
| `R2_ACCESS_KEY` | Optional | R2 access key |
| `R2_SECRET_KEY` | Optional | R2 secret key |
| `R2_BUCKET` | Optional | R2 bucket name |

## Common Operations

```bash
# Start API server (6 migrations run on startup, idempotent)
uvicorn src.api.app:app --reload --port 8001

# Pass 1 batch scan (NAIP NDVI + FEMA)
PYTHONPATH=. python scripts/batch_ndvi_scan.py --county Gaston --state NC --limit 100

# Pass 1.5 historical slopes + composite
PYTHONPATH=. python scripts/batch_historical_slope.py --county Gaston --state NC

# Pass 1.5b Sentinel enrichment (sentinel_worthy parcels)
PYTHONPATH=. python scripts/batch_sentinel_enrich.py --county Gaston --state NC --limit 50

# Pass 2 USPS vacancy enrichment
PYTHONPATH=. python -u scripts/batch_usps_enrich.py --county Gaston --state NC --min-composite 7.5

# Pass 2.5 Conviction Score Fusion
PYTHONPATH=. python scripts/batch_conviction_score.py --county Gaston --state NC

# USPS single address test
PYTHONPATH=. python scripts/test_usps_single.py --situs "4336 LA BREA DR CHARLOTTE NC"

# Register signal types (one-time)
PYTHONPATH=. python scripts/register_signal_types.py
```

## Pipeline Flow

1. **Load parcels**: Dashboard → `/parcels?county=Gaston&state=NC` → gis_parcels_core query
2. **Pass 1 batch**: `batch_ndvi_scan.py` → `naip_ndvi_fast()` + `fema_flood()` per parcel → `generate_all_flags()` → `batch_update_scan_results()` → scan_pass=1
3. **Pass 1.5 slopes**: `batch_historical_slope.py` → Planetary Computer STAC + COG reads → `compute_ndvi_slope()` → `compute_composite_scores()` → ndvi_slope_5yr + distress_composite
4. **Pass 1.5b Sentinel**: `batch_sentinel_enrich.py` → `enrich_sentinel()` per sentinel_worthy parcel → scan_pass=2
5. **Pass 2 USPS**: `batch_usps_enrich.py` → USPS API v3 (60 req/hr, jitter) → vacancy flags → DB persist. Mailing address fallback when Nominatim fails (same-state only).
6. **Pass 2.5 Conviction**: `batch_conviction_score.py` → canonical JOIN (county+state compound key) → reweighted DS+MC+VAC formula → conviction_score + motivation_scores backfill
7. **Dashboard filters**: VEG/NEG/FLD/STR/VAC pills, FEMA zones, score range, conviction min, property class, year built, absentee, show mode.
8. **Individual scan**: Click "SCAN+" → `/scan_distress` → full NAIP+Sentinel+FEMA+Planet
9. **Assessment**: Auto-generated narrative from all data sources incl conviction. Export CSV with 40+ columns.

## Data Volumes

- **Gaston County**: 225K total parcels, 141K "Residential 1 Family", 72.7K with coords (scannable)
  - Pass 1 COMPLETE: 72,714 scanned, 22.3% flagged, 0 errors, 128 min @ 9.4/sec
  - Pass 1.5 slopes COMPLETE: 72,708 slopes computed, 560 min @ 2.2/sec, 72,834 composites
  - Pass 2 USPS COMPLETE: 1,357 checked (composite>=7.5), 46 vacant (3.4%), 31 errors, 8 skipped (out-of-state)
  - Pass 2.5 conviction COMPLETE: 77,869 scored, 10,183 MC-joined, 208.9s
  - Composite distribution: 9% HIGH (>=7), 28.5% MODERATE (5-6.9), 28.4% LOW (3-4.9), 34% MINIMAL (<3)
- **Mecklenburg County**: ~6K+ scanned parcels (test batches only), not full pipeline
- **CDSE budget**: ~10K requests/month. Stats-only = 1 req/parcel.
- **Batch scan rates**: ~9.4/sec (Pass 1 NAIP+FEMA), ~2.2/sec (historical slope), ~60/hr (USPS per key)
- **Property classes**: Gaston='Residential 1 Family', Mecklenburg='Single-Family'

## Gotchas

- `psycopg2.extras.execute_batch` rowcount is unreliable — return `len(results)` instead
- NAIP `requests.Session` is thread-safe for GETs — safe for batch concurrency
- `load_dotenv()` crashes in heredoc/stdin mode — always use script files
- Sequential `parcel_id` order clusters geographically — use `ORDER BY md5(parcel_id)` for deterministic diversity (NOT `ORDER BY RANDOM()` — too expensive at scale)
- `nonlocal` cannot reference module-level globals in Python — use `global` instead
- `veg_confidence` must use `max()` not `or` — Python `or` treats 0.0 as falsy
- SentinelHub rate limits during batch runs are handled by the client's automatic retry
- **Google Street View embed**: Embed API (`/embed/v1/streetview`) is free but always faces default heading (direction car was driving). Must use JS API with `StreetViewService.getPanorama()` + `geometry.spherical.computeHeading(cameraPos, propertyPos)` to face the house. `/config` endpoint serves the key to the frontend. $200/mo free credit covers ~14K loads/mo.
- **Google Maps Embed API** (iframes) is free/unlimited. JS API with Street View is $14/1K loads but has $200/mo credit.
- **Railway PostgreSQL idle timeout**: Long-running batch jobs MUST use fresh `get_db_connection()` per flush/write — a single long-lived conn dies after ~60s idle.
- **Migration lock fix**: All `migrate_add_*_columns()` check `information_schema.columns` FIRST — skip ALTER TABLE DDL (ACCESS EXCLUSIVE lock) when columns already exist. Killed processes leave orphaned DB connections holding locks; use `pg_terminate_backend()` to clear.
- **STAC year types**: Planetary Computer `naip:year` may return string or int. Always normalize: `int(year_raw)`.
- **NAIP tile-boundary dedup**: Points on tile edges get 2 STAC items per year. Dedup with `seen_years` set.
- **USPS mailing address fallback**: When Nominatim can't resolve city/zip, use `mailing_city`/`mailing_zip` from GIS data — but ONLY when `mailing_state` matches property state. Out-of-state investor-owned properties get skipped.
- **Cross-county parcel_id collision**: 1,870 duplicate parcel_ids across counties. Must always JOIN through `counties.name + state_code` compound key, never bare parcel_id.
- **motivation_scores uniqueness**: Constraint is `(parcel_id, computed_at)`, NOT `parcel_id` alone. Use county-scoped DELETE + INSERT, not ON CONFLICT.
- **USPS nested object keys**: API returns `usps.usps_address`, `usps.usps_city`, `usps.usps_zip` (prefixed), NOT `usps.address`. Dashboard/export must use prefixed keys.
