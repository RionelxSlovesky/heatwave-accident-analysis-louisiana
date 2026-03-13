import time
import requests
import pandas as pd
from pathlib import Path
from config import (
    PROJECT_ROOT,
    NOAA_TOKEN,
    NOAA_BASE_URL,
    NOAA_DATASET_ID,
    LOUISIANA_LOCATION_ID,
)

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

HEADERS = {"token": NOAA_TOKEN}


def get_all_stations():
    all_results = []
    offset = 1

    while True:
        params = {
            "datasetid": NOAA_DATASET_ID,
            "locationid": LOUISIANA_LOCATION_ID,
            "limit": 1000,
            "offset": offset,
            "sortfield": "name",
            "sortorder": "asc",
        }

        response = requests.get(
            f"{NOAA_BASE_URL}/stations",
            headers=HEADERS,
            params=params,
            timeout=60,
        )
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
    stations = get_all_stations()
    stations_df = pd.DataFrame(stations)

    output_file = DATA_RAW / "louisiana_stations.csv"
    stations_df.to_csv(output_file, index=False)

    print(f"Saved Louisiana stations to: {output_file}")
    print(f"Total stations: {len(stations_df)}")

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

    print(f"Saved Louisiana stations to: {output_file}")


if __name__ == "__main__":
    main()
