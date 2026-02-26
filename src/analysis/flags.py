"""
Distress signal flag evaluation.

Combines NAIP baseline, Sentinel-2 trends, and FEMA flood data
into actionable distress flags with confidence scores.

Each evaluator returns:
    {"flag": bool, "confidence": float 0-1, "signal_code": str, "evidence": dict}
"""

import structlog

logger = structlog.get_logger("analysis.flags")

# --- Thresholds (tuned for RE distress detection) ---

# Vegetation neglect (bare/abandoned lots)
NDVI_NEGLECT_MIN = 0.10              # Below this = impervious/rock (not neglect)
NDVI_NEGLECT_MAX = 0.30              # NDVI 0.10-0.30 = neglect band

# Vegetation overgrowth (two-tier)
NDVI_OVERGROWTH_MODERATE = 0.50      # Moderate overgrowth threshold
NDVI_OVERGROWTH_STRONG = 0.65        # Strong overgrowth — high confidence
NDVI_OVERGROWTH_CHANGE = 0.15        # Delta above baseline mean
NDVI_TREND_INCREASING_SLOPE = 0.005  # Sentinel monthly slope threshold

# Structural change (demolition, fire, clearing)
NDVI_DROP_THRESHOLD = 0.20           # NDVI decrease indicating structural event
NDVI_LOW_ABSOLUTE = 0.20             # Very low NDVI = bare/impervious

# Flood risk
FLOOD_HIGH_CONFIDENCE = 1.0
FLOOD_MODERATE_CONFIDENCE = 0.6


def evaluate_vegetation_overgrowth(naip: dict | None, sentinel: dict | None) -> dict:
    """
    Detect vegetation overgrowth by combining NAIP baseline and Sentinel trends.

    Signals property neglect: yards/lots becoming overgrown.

    Logic:
    - NAIP: current NDVI > historical mean + threshold AND > absolute min
    - Sentinel: upward NDVI trend confirms growth over recent months
    - Both agreeing boosts confidence
    """
    result = {
        "flag": False,
        "confidence": 0.0,
        "signal_code": "vegetation_overgrowth",
        "evidence": {},
    }

    naip_flag = False
    naip_conf = 0.0
    sentinel_flag = False
    sentinel_conf = 0.0

    # NAIP baseline check (two-tier: strong >0.65, moderate 0.50-0.65)
    if naip and not naip.get("errors"):
        current = naip.get("current_ndvi")
        hist_mean = naip.get("mean_historical_ndvi")

        if current is not None and current > NDVI_OVERGROWTH_STRONG:
            # Strong overgrowth tier
            if hist_mean is not None and current > hist_mean + NDVI_OVERGROWTH_CHANGE:
                naip_flag = True
                naip_conf = min((current - hist_mean) / 0.3, 1.0)
                result["evidence"]["naip_current_ndvi"] = current
                result["evidence"]["naip_historical_mean"] = hist_mean
                result["evidence"]["naip_delta"] = round(current - hist_mean, 4)
                result["evidence"]["tier"] = "strong"
            elif hist_mean is None:
                # No history but very high NDVI — still flag with moderate confidence
                naip_flag = True
                naip_conf = 0.6
                result["evidence"]["naip_current_ndvi"] = current
                result["evidence"]["note"] = "no_historical_baseline"
                result["evidence"]["tier"] = "strong"
        elif current is not None and current > NDVI_OVERGROWTH_MODERATE:
            # Moderate overgrowth tier — only flag if historical delta confirms
            if hist_mean is not None and current > hist_mean + NDVI_OVERGROWTH_CHANGE:
                naip_flag = True
                naip_conf = min((current - hist_mean) / 0.3, 0.8)
                result["evidence"]["naip_current_ndvi"] = current
                result["evidence"]["naip_historical_mean"] = hist_mean
                result["evidence"]["naip_delta"] = round(current - hist_mean, 4)
                result["evidence"]["tier"] = "moderate"

    # Sentinel trend check
    if sentinel and not sentinel.get("errors"):
        slope = sentinel.get("trend_slope")
        direction = sentinel.get("trend_direction")
        latest = sentinel.get("latest_ndvi")

        if direction == "increasing" and slope is not None:
            if latest and latest > NDVI_OVERGROWTH_MODERATE:
                sentinel_flag = True
                sentinel_conf = min(slope / 0.02, 1.0)
                result["evidence"]["sentinel_slope"] = slope
                result["evidence"]["sentinel_direction"] = direction
                result["evidence"]["sentinel_latest_ndvi"] = latest

    # Combine sources
    if naip_flag and sentinel_flag:
        # Both agree — high confidence
        result["flag"] = True
        result["confidence"] = min(max(naip_conf, sentinel_conf) + 0.2, 1.0)
        result["evidence"]["agreement"] = "naip_and_sentinel"
    elif naip_flag:
        result["flag"] = True
        # Strong tier without history already has conservative confidence — no discount
        if result["evidence"].get("tier") == "strong" and result["evidence"].get("note") == "no_historical_baseline":
            result["confidence"] = naip_conf
        else:
            result["confidence"] = naip_conf * 0.8  # Slight discount for single source
        result["evidence"]["source"] = "naip_only"
    elif sentinel_flag:
        result["flag"] = True
        result["confidence"] = sentinel_conf * 0.7  # Less confident without NAIP baseline
        result["evidence"]["source"] = "sentinel_only"

    return result


def evaluate_flood_risk(fema: dict | None) -> dict:
    """
    Evaluate flood risk from FEMA NFHL data.

    Straightforward zone-based classification.
    """
    result = {
        "flag": False,
        "confidence": 0.0,
        "signal_code": "flood_risk",
        "evidence": {},
    }

    if not fema or fema.get("errors"):
        return result

    risk = fema.get("risk_level", "unknown")
    zone = fema.get("flood_zone")
    sfha = fema.get("is_sfha", False)

    if risk == "high" or sfha:
        result["flag"] = True
        result["confidence"] = FLOOD_HIGH_CONFIDENCE
    elif risk == "moderate":
        result["flag"] = True
        result["confidence"] = FLOOD_MODERATE_CONFIDENCE

    if result["flag"]:
        result["evidence"] = {
            "flood_zone": zone,
            "risk_level": risk,
            "is_sfha": sfha,
            "zone_subtype": fema.get("zone_subtype"),
        }
        if fema.get("map_url"):
            result["evidence"]["map_url"] = fema["map_url"]

    return result


def evaluate_structural_change(naip: dict | None, sentinel: dict | None) -> dict:
    """
    Detect structural change (demolition, fire, clearing).

    Signals: significant NDVI drop or very low current NDVI compared to baseline.
    This catches properties where vegetation/structures were removed.
    """
    result = {
        "flag": False,
        "confidence": 0.0,
        "signal_code": "structural_change",
        "evidence": {},
    }

    naip_flag = False
    naip_conf = 0.0
    sentinel_flag = False
    sentinel_conf = 0.0

    # NAIP: check for big drop from historical
    if naip and not naip.get("errors"):
        current = naip.get("current_ndvi")
        hist_mean = naip.get("mean_historical_ndvi")

        if current is not None and hist_mean is not None:
            drop = hist_mean - current
            if drop > NDVI_DROP_THRESHOLD:
                naip_flag = True
                naip_conf = min(drop / 0.4, 1.0)
                result["evidence"]["naip_current_ndvi"] = current
                result["evidence"]["naip_historical_mean"] = hist_mean
                result["evidence"]["naip_drop"] = round(drop, 4)

    # Sentinel: decreasing trend with significant slope
    if sentinel and not sentinel.get("errors"):
        slope = sentinel.get("trend_slope")
        direction = sentinel.get("trend_direction")
        latest = sentinel.get("latest_ndvi")
        earliest = sentinel.get("earliest_ndvi")

        if direction == "decreasing" and slope is not None:
            if earliest and latest and (earliest - latest) > NDVI_DROP_THRESHOLD:
                sentinel_flag = True
                sentinel_conf = min(abs(slope) / 0.02, 1.0)
                result["evidence"]["sentinel_slope"] = slope
                result["evidence"]["sentinel_drop"] = round(earliest - latest, 4)
                result["evidence"]["sentinel_latest_ndvi"] = latest

    # Combine
    if naip_flag and sentinel_flag:
        result["flag"] = True
        result["confidence"] = min(max(naip_conf, sentinel_conf) + 0.2, 1.0)
        result["evidence"]["agreement"] = "naip_and_sentinel"
    elif naip_flag:
        result["flag"] = True
        result["confidence"] = naip_conf * 0.8
        result["evidence"]["source"] = "naip_only"
    elif sentinel_flag:
        result["flag"] = True
        result["confidence"] = sentinel_conf * 0.7
        result["evidence"]["source"] = "sentinel_only"

    return result


def evaluate_vegetation_neglect(naip: dict | None, fema: dict | None) -> dict:
    """
    Detect vegetation neglect (bare/abandoned lots).

    NDVI 0.10-0.30 = minimal vegetation suggesting abandonment or severe neglect.
    Confidence is inversely proportional to NDVI in that band.
    Boosted if also in a flood zone (compounding distress).
    """
    result = {
        "flag": False,
        "confidence": 0.0,
        "signal_code": "vegetation_neglect",
        "evidence": {},
    }

    if not naip or naip.get("errors"):
        return result

    current = naip.get("current_ndvi")
    if current is None:
        return result

    if NDVI_NEGLECT_MIN <= current <= NDVI_NEGLECT_MAX:
        result["flag"] = True
        # Lower NDVI in band = higher confidence of neglect
        # 0.10 → conf 1.0, 0.30 → conf 0.3
        result["confidence"] = round(1.0 - ((current - NDVI_NEGLECT_MIN) /
                                             (NDVI_NEGLECT_MAX - NDVI_NEGLECT_MIN)) * 0.7, 2)
        result["evidence"]["naip_current_ndvi"] = current
        result["evidence"]["category"] = "neglect"

        # Boost if in flood zone (compounding distress)
        if fema and not fema.get("errors"):
            risk = fema.get("risk_level", "unknown")
            if risk in ("high", "moderate"):
                result["confidence"] = min(result["confidence"] + 0.15, 1.0)
                result["evidence"]["flood_boost"] = True
                result["evidence"]["flood_risk"] = risk

    return result


def evaluate_usps_vacancy(usps: dict | None) -> dict:
    """
    Evaluate USPS vacancy flag.

    USPS mail carriers flag an address as vacant after 90+ days of no
    mail collection. Carrier-confirmed — doesn't rely on imagery.

    Confidence levels:
    - vacant + dpv_confirmed: 0.90 (carrier saw it, address is real)
    - vacant + dpv unknown:   0.75 (vacant but address questionable)
    - vacant + addr mismatch: capped at 0.70 (USPS corrected our address)
    """
    result = {
        "flag": False,
        "confidence": 0.0,
        "signal_code": "usps_vacancy",
        "evidence": {},
    }

    if not usps:
        return result

    vacant = usps.get("vacant")
    if vacant is not True:
        return result

    confidence = 0.85

    dpv = usps.get("dpv_confirmed")
    mismatch = usps.get("address_mismatch", False)

    if dpv is True:
        confidence = 0.90
    elif dpv is False or dpv is None:
        confidence = 0.75

    if mismatch:
        confidence = min(confidence, 0.70)

    result["flag"] = True
    result["confidence"] = confidence
    result["evidence"] = {
        "source": "usps_address_api_v3",
        "vacant": True,
        "dpv_confirmed": dpv,
        "address_mismatch": mismatch,
        "usps_address": usps.get("usps_address"),
        "usps_city": usps.get("usps_city"),
        "usps_zip": usps.get("usps_zip"),
        "carrier_route": usps.get("carrier_route"),
    }

    return result


def generate_all_flags(naip: dict | None, sentinel: dict | None,
                       fema: dict | None, usps: dict | None = None) -> list[dict]:
    """
    Run all flag evaluators and return list of triggered flags.

    Returns list of dicts, each with:
        signal_code, flag, confidence, evidence
    Only returns flags where flag=True.
    """
    evaluators = [
        evaluate_vegetation_overgrowth(naip, sentinel),
        evaluate_vegetation_neglect(naip, fema),
        evaluate_flood_risk(fema),
        evaluate_structural_change(naip, sentinel),
        evaluate_usps_vacancy(usps),
    ]

    flags = [f for f in evaluators if f["flag"]]

    logger.info("flags_evaluated",
                total_checked=len(evaluators),
                flags_triggered=len(flags),
                codes=[f["signal_code"] for f in flags])

    return flags
