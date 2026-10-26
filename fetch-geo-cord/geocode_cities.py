#!/usr/bin/env python3
"""
Geocode city/destination rows in an Excel workbook using OpenCage.
Free-trial safe defaults: max 1 request/second, with a pause between requests.

Setup:
  python3 -m pip install openpyxl requests
  export OPENCAGE_API_KEY="YOUR_KEY"
  python3 geocode_cities.py input.xlsx output_with_coordinates.xlsx

The script preserves existing columns and adds:
  Latitude, Longitude, Geocode Status, Matched Location, Confidence

It saves after every row so progress is retained if interrupted.
"""
import os
import sys
import time
import requests
from openpyxl import load_workbook

API_URL = "https://api.opencagedata.com/geocode/v1/json"
REQUEST_INTERVAL_SECONDS = 1.05
MAX_RETRIES = 3

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 geocode_cities.py input.xlsx output.xlsx")
        sys.exit(2)

    api_key = os.getenv("OPENCAGE_API_KEY")
    if not api_key:
        print('ERROR: Set OPENCAGE_API_KEY first, e.g. export OPENCAGE_API_KEY="YOUR_KEY"')
        sys.exit(2)

    input_path, output_path = sys.argv[1], sys.argv[2]
    wb = load_workbook(input_path)
    ws = wb.active

    headers = {str(ws.cell(1, c).value).strip(): c for c in range(1, ws.max_column + 1)
               if ws.cell(1, c).value is not None}
    if "Country" not in headers or "City" not in headers:
        raise ValueError("Expected columns named 'Country' and 'City' in the first row.")

    def ensure_col(name):
        if name in headers:
            return headers[name]
        col = ws.max_column + 1
        ws.cell(1, col).value = name
        headers[name] = col
        return col

    lat_col = ensure_col("Latitude")
    lon_col = ensure_col("Longitude")
    status_col = ensure_col("Geocode Status")
    match_col = ensure_col("Matched Location")
    confidence_col = ensure_col("Confidence")

    session = requests.Session()
    last_request_at = 0.0
    total = ws.max_row - 1

    for row in range(2, ws.max_row + 1):
        city = ws.cell(row, headers["City"]).value
        country = ws.cell(row, headers["Country"]).value

        # Skip already completed rows, allowing the script to resume.
        if ws.cell(row, status_col).value in ("OK", "NO_MATCH", "ERROR"):
            continue

        if not city or not country:
            ws.cell(row, status_col).value = "MISSING_CITY_OR_COUNTRY"
            wb.save(output_path)
            continue

        # Include the country to reduce ambiguity for cities with the same name.
        query = f"{str(city).strip()}, {str(country).strip()}"
        params = {
            "q": query,
            "key": api_key,
            "limit": 1,
            "no_annotations": 1,
        }

        result = None
        error_status = None
        for attempt in range(MAX_RETRIES):
            elapsed = time.monotonic() - last_request_at
            if elapsed < REQUEST_INTERVAL_SECONDS:
                time.sleep(REQUEST_INTERVAL_SECONDS - elapsed)

            try:
                response = session.get(API_URL, params=params, timeout=30)
                last_request_at = time.monotonic()
                if response.status_code == 429:
                    time.sleep(2 + attempt * 2)
                    continue
                if response.status_code in (500, 502, 503, 504):
                    time.sleep(2 + attempt * 2)
                    continue
                response.raise_for_status()
                result = response.json()
                break
            except requests.RequestException as exc:
                error_status = f"HTTP_ERROR: {exc}"
                time.sleep(2 + attempt * 2)

        if result is None:
            ws.cell(row, status_col).value = error_status or "ERROR: no response"
        else:
            code = result.get("status", {}).get("code")
            matches = result.get("results", [])
            if code == 200 and matches:
                item = matches[0]
                geometry = item.get("geometry", {})
                components = item.get("components", {})
                lat, lon = geometry.get("lat"), geometry.get("lng")
                if lat is not None and lon is not None:
                    ws.cell(row, lat_col).value = lat
                    ws.cell(row, lon_col).value = lon
                    ws.cell(row, status_col).value = "OK"
                    ws.cell(row, match_col).value = item.get("formatted", "")
                    ws.cell(row, confidence_col).value = item.get("confidence", "")
                    # Flag likely mismatches for manual review.
                    country_component = str(components.get("country", "")).casefold()
                    expected_country = str(country).strip().casefold()
                    if expected_country and country_component and (
                        expected_country not in country_component
                        and country_component not in expected_country
                    ):
                        ws.cell(row, status_col).value = "REVIEW_COUNTRY_MISMATCH"
                else:
                    ws.cell(row, status_col).value = "NO_COORDINATES"
            elif code == 429:
                ws.cell(row, status_col).value = "RATE_LIMITED"
            elif code == 402:
                ws.cell(row, status_col).value = "QUOTA_EXCEEDED"
                wb.save(output_path)
                print("Daily quota reached; progress saved. Run again after quota resets.")
                sys.exit(1)
            else:
                ws.cell(row, status_col).value = "NO_MATCH" if not matches else f"API_STATUS_{code}"

        wb.save(output_path)
        status = ws.cell(row, status_col).value
        print(f"[{row-1}/{total}] {query} -> {status}")

    print(f"\nFinished. Output saved to: {output_path}")
    print("Review rows with NO_MATCH, REVIEW_COUNTRY_MISMATCH, or other non-OK statuses.")

if __name__ == "__main__":
    main()
