# Distress Scanner

Standalone Python service that analyzes free satellite/aerial imagery (NAIP, Sentinel-2, FEMA flood data) to detect early property distress signals and writes them into the shared Railway PostgreSQL database used by Motivated Curator.

## Database Contract
- **Shared DB**: Same Railway PostgreSQL as Motivated Curator (connection via `DATABASE_URL` in `.env`)
- **Reads from**: `gis_parcels_core`, `counties`, `parcels`
- **Writes to**: `signal_types`, `parcel_signals`
- **Signal types**: `vegetation_overgrowth` (2.0), `flood_risk` (1.5), `structural_change` (2.5)

## Data Sources
- **NAIP**: USGS ArcGIS REST (free, no key) — multi-year NDVI baselines
- **Sentinel-2**: Copernicus CDSE via sentinelhub-py (free tier, OAuth) — monthly NDVI trends
- **FEMA**: ArcGIS REST (free, no key) — flood zone classification

## Running
```bash
# Install dependencies
pip install -r requirements.txt

# Register signal types (one-time)
python scripts/register_signal_types.py

# Test NAIP on sample locations
python scripts/test_sample.py

# Start API server
uvicorn src.api.app:app --reload

# Batch county scan
python scripts/run_county_scan.py --county Mecklenburg --state NC --limit 10
```

## Environment Variables
- `DATABASE_URL` — PostgreSQL connection string (required)
- `SH_CLIENT_ID` — Sentinel Hub OAuth client ID (Phase 2)
- `SH_CLIENT_SECRET` — Sentinel Hub OAuth client secret (Phase 2)

## Key Thresholds
- NDVI overgrowth: current > historical_mean + 0.15 AND current > 0.50
- Flood risk: FEMA zones A, AE, AH, AO, AR, V, VE
- Structural change: NDVI drop > 0.20 from historical baseline
