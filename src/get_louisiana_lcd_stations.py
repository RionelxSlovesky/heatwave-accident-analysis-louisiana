import time
import requests
import pandas as pd
from config import PROJECT_ROOT, NOAA_TOKEN, NOAA_BASE_URL

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_RAW / "louisiana_lcd_stations.csv"

HEADERS = {"token": NOAA_TOKEN}


def get_all_pages(endpoint, params):
    all_results = []
    offset = 1

    while True:
        query = params.copy()
        query["limit"] = 1000
        query["offset"] = offset

        response = requests.get(
            f"{NOAA_BASE_URL}/{endpoint}",
            headers=HEADERS,
            params=query,
            timeout=60,
        )

        if response.status_code in [429, 500, 502, 503, 504]:
            print(
                f"Temporary NOAA error {response.status_code}. Sleeping for 10 seconds..."
            )
            time.sleep(10)
            continue

        response.raise_for_status()
        payload = response.json()

        results = payload.get("results", [])
        if not results:
            break

        all_results.extend(results)

        metadata = payload.get("metadata", {}).get("resultset", {})
        count = metadata.get("count", 0)

        offset += len(results)
        if offset > count:
            break

        time.sleep(0.25)

    return all_results


def main():
    stations = get_all_pages(
        "stations",
        {
            "datasetid": "LCD",
            "locationid": "FIPS:22",
            "sortfield": "name",
            "sortorder": "asc",
        },
    )

    stations_df = pd.DataFrame(stations)
    stations_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved Louisiana LCD stations to: {OUTPUT_FILE}")
    print(f"Total LCD stations: {len(stations_df)}")

    keep_cols = [
        col
        for col in [
            "id",
            "name",
            "latitude",
            "longitude",
            "mindate",
            "maxdate",
            "datacoverage",
        ]
        if col in stations_df.columns
    ]
    print("\nPreview:")
    print(stations_df[keep_cols].head())


if __name__ == "__main__":
    main()