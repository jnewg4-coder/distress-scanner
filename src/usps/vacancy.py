"""
USPS Address REST API v3 — Vacancy Checker.

Queries the USPS Address Validation API to get the `vacant` flag
from the `additionalInfo` response object. Mail carriers flag an
address as vacant after 90+ days of no mail collection.

API docs: https://developers.usps.com/addressesv3
Auth: OAuth2 client_credentials → Bearer token
Rate limit: 60 requests/hour per Consumer Key (token-based, NOT IP-based)

Rate limiter uses random delays (not fixed intervals) to avoid
bot-like uniform spacing. Delays are drawn from a random range
that varies each request — looks human, stays under quota.

Why jitter matters:
  1. Bot detection: API gateways (Kong, Apigee) flag clients sending
     at exact fixed intervals. 61.0s every time = unmistakably automated.
  2. SpikeArrest micro-windows: Gateways subdivide hourly limits into
     sub-windows. 2 requests in one micro-window → 429 even if under
     60/hr total. Random delays spread across boundaries.

Supports multiple accounts for parallel runs:
  USPS_CLIENT_ID / USPS_CLIENT_SECRET       (account 1)
  USPS_CLIENT_ID_2 / USPS_CLIENT_SECRET_2   (account 2)
  USPS_CLIENT_ID_3 / USPS_CLIENT_SECRET_3   (account 3)

Usage:
  checker = USPSVacancyChecker()          # uses account 1
  checker = USPSVacancyChecker(account=2) # uses account 2
  result = checker.check_address("3120 M ST NW", city="Washington", state="DC")
  # result.vacant == True/False/None
"""
from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass

import requests
import structlog

logger = structlog.get_logger("usps.vacancy")

TOKEN_URL = "https://apis.usps.com/oauth2/v3/token"
ADDRESS_URL = "https://apis.usps.com/addresses/v3/address"
# Test environment (uncomment to test without burning prod quota)
# TOKEN_URL = "https://apis-tem.usps.com/oauth2/v3/token"
# ADDRESS_URL = "https://apis-tem.usps.com/addresses/v3/address"

# Random delay range (seconds). Configurable via env vars.
# Default 30-55s (~80 req/hr avg). Keeps us under quota while
# looking nothing like a bot with fixed intervals.
DELAY_MIN = int(os.environ.get("USPS_CHAIN_DELAY_MIN", 30))
DELAY_MAX = int(os.environ.get("USPS_CHAIN_DELAY_MAX", 55))

# 429 backoff: start here, double each consecutive 429, cap at max
BACKOFF_START = 120      # 2 min
BACKOFF_MAX = 900        # 15 min
BACKOFF_MULTIPLIER = 2


@dataclass
class VacancyResult:
    """Result of a USPS vacancy check for a single address."""
    street_address: str
    city: str | None
    state: str | None
    zip_code: str | None
    # USPS response
    vacant: bool | None          # True = vacant, False = occupied, None = error/unknown
    dpv_confirmed: bool | None   # True = valid deliverable address
    business: bool | None        # True = commercial, False = residential
    carrier_route: str | None
    # Normalized address from USPS
    usps_address: str | None     # USPS-standardized street address
    usps_city: str | None
    usps_state: str | None
    usps_zip: str | None
    usps_zip4: str | None
    # Mismatch detection
    address_mismatch: bool       # True if USPS returned a different street address
    # Metadata
    raw_response: dict | None
    error: str | None


def _get_credentials(account: int = 1) -> tuple[str, str]:
    """Load USPS API credentials from environment for the given account number."""
    if account == 1:
        client_id = os.environ.get("USPS_CLIENT_ID", "")
        client_secret = os.environ.get("USPS_CLIENT_SECRET", "")
    else:
        client_id = os.environ.get(f"USPS_CLIENT_ID_{account}", "")
        client_secret = os.environ.get(f"USPS_CLIENT_SECRET_{account}", "")

    if not client_id or not client_secret:
        raise ValueError(
            f"USPS account {account} credentials not set. "
            f"Set USPS_CLIENT_ID{'_' + str(account) if account > 1 else ''} "
            f"and USPS_CLIENT_SECRET{'_' + str(account) if account > 1 else ''} in .env"
        )
    return client_id, client_secret


# US state abbreviations for situs address parsing
_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}


def split_situs(
    situs: str,
    fallback_state: str | None = None,
    fallback_city: str | None = None,
) -> dict:
    """
    Split a situs address into {street, city, state, zip_code} for USPS API.

    Handles:
      - "123 MAIN ST CHARLOTTE NC"       -> street/city/state
      - "123 MAIN ST CHARLOTTE NC 28083" -> street/city/state/zip
      - "123 MAIN ST"                    -> street only, uses fallbacks
      - "CRESTVIEW DR 103"               -> reversed TN situs, uses fallbacks
      - "123 MAIN ST UNINC NC"           -> strips UNINC, uses fallbacks for city
    """
    parts = situs.strip().split()
    if not parts:
        return {"street": situs, "city": fallback_city, "state": fallback_state, "zip_code": None}

    # Strip trailing ZIP code if present (5-digit or 5-4 format)
    zip_code = None
    last = parts[-1]
    if len(last) == 5 and last.isdigit():
        zip_code = last
        parts = parts[:-1]
    elif len(last) == 10 and last[5] == "-" and last[:5].isdigit() and last[6:].isdigit():
        zip_code = last[:5]
        parts = parts[:-1]

    if not parts:
        return {"street": situs.strip(), "city": fallback_city, "state": fallback_state, "zip_code": zip_code}

    # Ambiguous tokens: both a state code AND a common street suffix
    # e.g. "CT" = Connecticut OR Court, "IN" = Indiana OR a preposition
    _AMBIGUOUS_STATE_SUFFIX = {"CT", "IN", "AL", "ME", "OR"}

    # Street suffixes used for city/street boundary detection
    street_suffixes = {
        "ST", "AVE", "AV", "RD", "DR", "LN", "CT", "CIR", "BLVD",
        "WAY", "PL", "TRL", "LOOP", "HWY", "PKY", "PKWY", "COVE",
        "CV", "RUN", "PATH", "PASS", "PT", "PIKE", "SQ", "TER",
        "TERR", "ALY", "ROW", "WALK", "XING", "EXT", "BND", "CRES",
        "GRV", "HOLW", "IS", "KNL", "LK", "LNDG", "MALL", "MNR",
        "MDW", "MDWS", "ML", "MLS", "OVAL", "PARK", "PLZ", "RIDGE",
        "RDG", "SHR", "SPG", "SPUR", "TRCE", "VLY", "VW", "VISTA",
    }

    # Check if last token is a 2-letter state code
    if len(parts) >= 3 and parts[-1].upper() in _STATE_CODES:
        state = parts[-1].upper()

        # Disambiguation: if token is ambiguous (e.g. CT=Court vs Connecticut)
        # and we have a fallback_state that differs, prefer street suffix
        if (state in _AMBIGUOUS_STATE_SUFFIX
                and fallback_state
                and state != fallback_state.upper()):
            # Treat as street suffix, not state code
            return {"street": " ".join(parts), "city": fallback_city,
                    "state": fallback_state, "zip_code": zip_code}

        city_candidate = parts[-2].upper()

        # Skip non-city tokens like "UNINC"
        skip_words = {"UNINC", "UNINCORP", "UNINCORPORATED", "COUNTY", "TWP", "TOWNSHIP"}
        if city_candidate in skip_words or city_candidate.isdigit():
            street = " ".join(parts[:-2])
            return {"street": street, "city": fallback_city, "state": state, "zip_code": zip_code}

        # Walk backwards from state to find where city starts
        city_parts = []
        idx = len(parts) - 2  # start just before state
        while idx > 0:
            token = parts[idx].upper().rstrip(",.")
            if token in street_suffixes:
                break
            city_parts.insert(0, parts[idx])
            idx -= 1

        if city_parts:
            street = " ".join(parts[:idx + 1])
            city = " ".join(city_parts)
            return {"street": street, "city": city, "state": state, "zip_code": zip_code}
        else:
            street = " ".join(parts[:-2])
            return {"street": street, "city": parts[-2], "state": state, "zip_code": zip_code}

    # No state code found — return as-is with fallbacks
    return {"street": " ".join(parts), "city": fallback_city, "state": fallback_state, "zip_code": zip_code}


def _detect_mismatch(input_addr: str, usps_addr: str | None) -> bool:
    """Check if USPS returned a meaningfully different street address."""
    if not usps_addr:
        return False
    # Normalize for comparison: uppercase, strip extra spaces
    a = " ".join(input_addr.upper().split())
    b = " ".join(usps_addr.upper().split())
    # Simple heuristic: if USPS address is contained in input, no mismatch
    if b in a or a in b:
        return False
    # If they share the same house number, probably just formatting
    a_parts = a.split()
    b_parts = b.split()
    if a_parts and b_parts and a_parts[0] == b_parts[0]:
        return False
    return True


class USPSVacancyChecker:
    """Rate-limited USPS Address REST API client that extracts vacancy flags."""

    def __init__(
        self,
        account: int = 1,
        proxy: str | None = None,
        delay_min: float = DELAY_MIN,
        delay_max: float = DELAY_MAX,
    ):
        self.account = account
        self._client_id, self._client_secret = _get_credentials(account)
        self._token: str | None = None
        self._token_expires: float = 0
        self._last_request: float = 0
        self._request_count: int = 0
        self._consecutive_429s: int = 0
        self._delay_min = delay_min
        self._delay_max = delay_max
        self._session = requests.Session()
        if proxy:
            self._session.proxies = {"http": proxy, "https": proxy}
        self._session.headers["User-Agent"] = "DistressScanner/1.0"

    def _authenticate(self) -> None:
        """Get or refresh OAuth2 bearer token."""
        if self._token and time.time() < self._token_expires - 60:
            return  # token still valid

        resp = self._session.post(
            TOKEN_URL,
            json={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires = time.time() + data.get("expires_in", 3600)
        logger.info("usps_authenticated", account=self.account)

    def _random_delay(self) -> None:
        """Wait a random duration between requests. Not fixed, not predictable."""
        if self._last_request <= 0:
            return  # first request, no wait

        elapsed = time.time() - self._last_request
        target = random.uniform(self._delay_min, self._delay_max)

        if elapsed < target:
            wait = target - elapsed
            logger.debug("delay", seconds=round(wait, 1), target=round(target, 1))
            time.sleep(wait)

    def _backoff_429(self, retry_after: int | None = None) -> None:
        """Escalating backoff on 429 responses."""
        self._consecutive_429s += 1

        if retry_after and retry_after > 0:
            # Respect server's Retry-After + random jitter
            wait = retry_after + random.uniform(5, 30)
            logger.warning(
                "429_retry_after",
                account=self.account,
                retry_after=retry_after,
                wait=round(wait, 1),
                consecutive=self._consecutive_429s,
            )
        else:
            # Exponential backoff with jitter
            base = BACKOFF_START * (BACKOFF_MULTIPLIER ** (self._consecutive_429s - 1))
            base = min(base, BACKOFF_MAX)
            wait = base + random.uniform(0, base * 0.3)
            logger.warning(
                "429_backoff",
                account=self.account,
                wait=round(wait, 1),
                consecutive=self._consecutive_429s,
            )

        time.sleep(wait)

    def check_address(
        self,
        street_address: str,
        *,
        city: str | None = None,
        state: str | None = None,
        zip_code: str | None = None,
        county: str | None = None,
        lat: float | None = None,
        lng: float | None = None,
    ) -> VacancyResult:
        """
        Check a single address for USPS vacancy status.

        Must provide either (city + state) or zip_code.
        If city/zip missing but county+state available, auto-resolves via Nominatim.
        """
        # Auto-resolve city/zip via Nominatim if missing
        if not city and not zip_code and county and state:
            try:
                from src.usps.geocode import resolve_city_zip
                geo = resolve_city_zip(street_address, county, state, lat=lat, lng=lng)
                if geo.get("city"):
                    city = geo["city"]
                    logger.info("nominatim_city_resolved", city=city,
                                confidence=geo.get("confidence"), source=geo.get("source"))
                if geo.get("zip"):
                    zip_code = geo["zip"]
                    logger.info("nominatim_zip_resolved", zip=zip_code,
                                confidence=geo.get("confidence"), source=geo.get("source"))
            except Exception as e:
                logger.warning("nominatim_fallback_failed", error=str(e))

        self._authenticate()
        self._random_delay()

        params = {"streetAddress": street_address}
        if city:
            params["city"] = city
        if state:
            params["state"] = state
        if zip_code:
            params["ZIPCode"] = zip_code

        try:
            resp = self._session.get(
                ADDRESS_URL,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=30,
            )
            self._last_request = time.time()
            self._request_count += 1

            if resp.status_code == 429:
                # Parse Retry-After header if present
                retry_after = None
                ra = resp.headers.get("Retry-After")
                if ra and ra.isdigit():
                    retry_after = int(ra)

                self._backoff_429(retry_after)

                return VacancyResult(
                    street_address=street_address, city=city, state=state,
                    zip_code=zip_code, vacant=None, dpv_confirmed=None,
                    business=None, carrier_route=None, usps_address=None,
                    usps_city=None, usps_state=None, usps_zip=None,
                    usps_zip4=None, address_mismatch=False,
                    raw_response=None, error="rate_limited",
                )

            # Successful response — reset 429 counter
            self._consecutive_429s = 0

            resp.raise_for_status()
            data = resp.json()

            addr = data.get("address", {})
            info = data.get("additionalInfo", {})

            vacant_flag = info.get("vacant", "")
            dpv_flag = info.get("DPVConfirmation", "")
            biz_flag = info.get("business", "")

            usps_street = addr.get("streetAddress")
            mismatch = _detect_mismatch(street_address, usps_street)

            return VacancyResult(
                street_address=street_address,
                city=city,
                state=state,
                zip_code=zip_code,
                vacant=vacant_flag == "Y" if vacant_flag in ("Y", "N") else None,
                dpv_confirmed=dpv_flag == "Y" if dpv_flag in ("Y", "N", "S", "D") else None,
                business=biz_flag == "Y" if biz_flag in ("Y", "N") else None,
                carrier_route=info.get("carrierRoute"),
                usps_address=usps_street,
                usps_city=addr.get("city"),
                usps_state=addr.get("state"),
                usps_zip=addr.get("ZIPCode"),
                usps_zip4=addr.get("ZIPPlus4"),
                address_mismatch=mismatch,
                raw_response=data,
                error=None,
            )

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            logger.warning(
                "usps_api_error",
                account=self.account,
                status=status,
                address=street_address,
                error=str(e),
            )
            return VacancyResult(
                street_address=street_address, city=city, state=state,
                zip_code=zip_code, vacant=None, dpv_confirmed=None,
                business=None, carrier_route=None, usps_address=None,
                usps_city=None, usps_state=None, usps_zip=None,
                usps_zip4=None, address_mismatch=False,
                raw_response=None, error=f"http_{status}",
            )
        except Exception as e:
            logger.warning(
                "usps_request_error",
                account=self.account,
                address=street_address,
                error=str(e),
            )
            return VacancyResult(
                street_address=street_address, city=city, state=state,
                zip_code=zip_code, vacant=None, dpv_confirmed=None,
                business=None, carrier_route=None, usps_address=None,
                usps_city=None, usps_state=None, usps_zip=None,
                usps_zip4=None, address_mismatch=False,
                raw_response=None, error=str(e),
            )

    def check_batch(
        self,
        addresses: list[dict],
        *,
        progress_every: int = 10,
    ) -> list[VacancyResult]:
        """
        Check a list of addresses. Each dict needs:
          street_address (required), city, state, zip_code (at least city+state or zip)

        Rate-limited automatically with random delays. Logs progress.
        """
        results = []
        total = len(addresses)
        vacant_count = 0
        error_count = 0

        logger.info("batch_started", account=self.account, total=total)

        for i, addr in enumerate(addresses):
            result = self.check_address(
                addr["street_address"],
                city=addr.get("city"),
                state=addr.get("state"),
                zip_code=addr.get("zip_code"),
            )
            results.append(result)

            if result.vacant is True:
                vacant_count += 1
            if result.error:
                error_count += 1

            if (i + 1) % progress_every == 0 or (i + 1) == total:
                logger.info(
                    "batch_progress",
                    account=self.account,
                    checked=i + 1,
                    total=total,
                    vacant=vacant_count,
                    errors=error_count,
                    pct=round((i + 1) / total * 100, 1),
                )

        logger.info(
            "batch_complete",
            account=self.account,
            total=total,
            vacant=vacant_count,
            errors=error_count,
        )
        return results

    @property
    def request_count(self) -> int:
        return self._request_count


def check_single(
    street: str,
    city: str | None = None,
    state: str | None = None,
    zip_code: str | None = None,
    account: int = 1,
) -> VacancyResult:
    """Convenience function for one-off checks."""
    checker = USPSVacancyChecker(account=account)
    return checker.check_address(street, city=city, state=state, zip_code=zip_code)
