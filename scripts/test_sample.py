#!/usr/bin/env python3
"""
Test NAIP baseline analysis on known sample locations.

Usage:
  python scripts/test_sample.py
"""

import sys
import os
import time
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from src.naip.baseline import naip_baseline
from src.naip.client import NAIPClient


SAMPLE_LOCATIONS = [
    {
        "name": "Charlotte Downtown (urban, low NDVI expected)",
        "lat": 35.2271,
        "lng": -80.8431,
    },
    {
        "name": "Rural Mecklenburg (farmland, moderate NDVI)",
        "lat": 35.35,
        "lng": -80.95,
    },
    {
        "name": "Davidson Residential (suburban, moderate NDVI)",
        "lat": 35.4993,
        "lng": -80.8487,
    },
]


def test_identify():
    """Quick test of the identify endpoint."""
    print("=" * 70)
    print("TEST 1: Raw identify endpoint")
    print("=" * 70)

    client = NAIPClient()
    lat, lng = 35.2271, -80.8431

    print(f"\nIdentifying point: ({lat}, {lng})")
    start = time.time()
    data = client.identify(lat, lng)
    elapsed = time.time() - start

    print(f"  Response time: {elapsed:.2f}s")
    print(f"  Value: {data.get('value', 'N/A')}")

    catalog = data.get("catalogItems", {})
    features = catalog.get("features", [])
    print(f"  Catalog items: {len(features)}")
    if features:
        attrs = features[0].get("attributes", {})
        print(f"  First item attrs: {json.dumps({k: v for k, v in list(attrs.items())[:8]}, indent=4)}")

    return data


def test_ndvi_point():
    """Test NDVI computation at a point."""
    print("\n" + "=" * 70)
    print("TEST 2: NDVI at point")
    print("=" * 70)

    client = NAIPClient()
    lat, lng = 35.2271, -80.8431

    print(f"\nComputing NDVI at ({lat}, {lng})...")
    start = time.time()
    result = client.compute_ndvi_at_point(lat, lng)
    elapsed = time.time() - start

    print(f"  Response time: {elapsed:.2f}s")
    print(f"  NDVI: {result.get('ndvi')}")
    print(f"  Bands: {result.get('bands')}")
    print(f"  Date: {result.get('acquisition_date')}")
    if result.get("error"):
        print(f"  Error: {result['error']}")

    ndvi = result.get("ndvi")
    if ndvi is not None:
        assert -1.0 <= ndvi <= 1.0, f"NDVI out of range: {ndvi}"
        print("  NDVI range check: PASS")

    return result


def test_available_years():
    """Test historical year availability queries."""
    print("\n" + "=" * 70)
    print("TEST 3: Available historical years")
    print("=" * 70)

    client = NAIPClient()
    lat, lng = 35.2271, -80.8431

    print(f"\nQuerying available years at ({lat}, {lng})...")
    start = time.time()
    years = client.get_available_years(lat, lng)
    elapsed = time.time() - start

    print(f"  Response time: {elapsed:.2f}s")
    print(f"  Available years: {len(years)}")
    for y in years:
        print(f"    Year {y['year']}: date={y['date']}")

    return years


def test_full_baseline():
    """Test the full naip_baseline function on sample locations."""
    print("\n" + "=" * 70)
    print("TEST 4: Full NAIP baseline analysis")
    print("=" * 70)

    for loc in SAMPLE_LOCATIONS:
        print(f"\n{'─' * 60}")
        print(f"Location: {loc['name']}")
        print(f"Coords:   ({loc['lat']}, {loc['lng']})")
        print(f"{'─' * 60}")

        start = time.time()
        result = naip_baseline(loc["lat"], loc["lng"])
        elapsed = time.time() - start

        print(f"  Time:            {elapsed:.2f}s")
        print(f"  Current NDVI:    {result['current_ndvi']}")
        print(f"  Current Date:    {result['current_date']}")
        print(f"  Assessment:      {result['vegetation_assessment']}")
        print(f"  Historical pts:  {len(result['historical_ndvi'])}")

        for h in result["historical_ndvi"]:
            ndvi_str = f"{h['ndvi']:.4f}" if h['ndvi'] is not None else "no data"
            print(f"    {h.get('year', '?')}: NDVI={ndvi_str}")

        print(f"  Mean Historical: {result['mean_historical_ndvi']}")
        print(f"  NDVI Change:     {result['ndvi_change']}")
        print(f"  Change %:        {result['ndvi_change_pct']}")
        print(f"  Trend:           {result['trend']}")
        print(f"  Overgrowth Flag: {result['overgrowth_flag']}")
        print(f"  Confidence:      {result['overgrowth_confidence']}")
        print(f"  Image URL:       {result['image_url']}")

        if result["errors"]:
            print(f"  Errors:")
            for e in result["errors"]:
                print(f"    - {e}")

        if result["current_ndvi"] is not None:
            assert -1.0 <= result["current_ndvi"] <= 1.0, f"NDVI out of range"
            print("  Validation: PASS")

        # Brief pause between locations
        time.sleep(1)


def main():
    print("NAIP Distress Scanner — Integration Tests")
    print(f"Testing against USGS NAIP ArcGIS REST endpoint\n")

    test_identify()
    test_ndvi_point()
    test_available_years()
    test_full_baseline()

    print("\n" + "=" * 70)
    print("ALL TESTS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
