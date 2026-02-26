#!/usr/bin/env python3
"""
Test USPS vacancy check on a single address.

Usage:
  PYTHONPATH=. python scripts/test_usps_single.py --street "4336 LA BREA DR" --city CHARLOTTE --state NC
  PYTHONPATH=. python scripts/test_usps_single.py --situs "4336 LA BREA DR CHARLOTTE NC"
  PYTHONPATH=. python scripts/test_usps_single.py --street "123 MAIN ST" --zip 28083
"""

import argparse
import json
import sys

from dotenv import load_dotenv
load_dotenv()

from src.usps.vacancy import USPSVacancyChecker, split_situs
from src.analysis.flags import evaluate_usps_vacancy


def main():
    parser = argparse.ArgumentParser(description="Test USPS vacancy check")
    parser.add_argument("--street", help="Street address (e.g. '123 MAIN ST')")
    parser.add_argument("--city", help="City name")
    parser.add_argument("--state", help="State code (e.g. NC)")
    parser.add_argument("--zip", help="ZIP code")
    parser.add_argument("--situs", help="Full situs address to auto-parse")
    parser.add_argument("--account", type=int, default=1, help="USPS account (default: 1)")
    args = parser.parse_args()

    # Parse situs if provided
    if args.situs:
        parsed = split_situs(args.situs, fallback_state=args.state, fallback_city=args.city)
        street = parsed["street"]
        city = parsed["city"] or args.city
        state = parsed["state"] or args.state
        zip_code = parsed.get("zip_code") or args.zip
        print(f"\nParsed situs: {json.dumps(parsed, indent=2)}")
    else:
        street = args.street
        city = args.city
        state = args.state
        zip_code = args.zip

    if not street:
        print("ERROR: Provide --street or --situs")
        sys.exit(1)
    if not (city and state) and not zip_code:
        print("ERROR: Provide (--city + --state) or --zip")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  USPS Vacancy Check")
    print(f"  Street:  {street}")
    print(f"  City:    {city}")
    print(f"  State:   {state}")
    print(f"  ZIP:     {zip_code}")
    print(f"  Account: {args.account}")
    print(f"{'='*60}\n")

    print("Calling USPS API...")
    checker = USPSVacancyChecker(account=args.account)
    result = checker.check_address(street, city=city, state=state, zip_code=zip_code)

    print(f"\n--- USPS Response ---")
    print(f"  Vacant:           {result.vacant}")
    print(f"  DPV Confirmed:    {result.dpv_confirmed}")
    print(f"  Business:         {result.business}")
    print(f"  Carrier Route:    {result.carrier_route}")
    print(f"  USPS Address:     {result.usps_address}")
    print(f"  USPS City:        {result.usps_city}")
    print(f"  USPS State:       {result.usps_state}")
    print(f"  USPS ZIP:         {result.usps_zip}")
    print(f"  USPS ZIP+4:       {result.usps_zip4}")
    print(f"  Address Mismatch: {result.address_mismatch}")
    print(f"  Error:            {result.error}")

    # Run flag evaluator
    usps_data = {
        "vacant": result.vacant,
        "dpv_confirmed": result.dpv_confirmed,
        "address_mismatch": result.address_mismatch,
        "usps_address": result.usps_address,
        "usps_city": result.usps_city,
        "usps_zip": result.usps_zip,
        "carrier_route": result.carrier_route,
    }
    flag = evaluate_usps_vacancy(usps_data)
    print(f"\n--- Flag Evaluation ---")
    print(f"  Flag:       {flag['flag']}")
    print(f"  Confidence: {flag['confidence']}")
    print(f"  Signal:     {flag['signal_code']}")

    if result.raw_response:
        print(f"\n--- Raw Response ---")
        print(json.dumps(result.raw_response, indent=2))

    print(f"\n{'='*60}")
    if result.vacant is True:
        print("  RESULT: VACANT <<<")
    elif result.vacant is False:
        print("  RESULT: OCCUPIED")
    elif result.error:
        print(f"  RESULT: ERROR ({result.error})")
    else:
        print("  RESULT: UNKNOWN")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
