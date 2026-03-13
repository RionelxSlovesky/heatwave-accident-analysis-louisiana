import time
import requests
import pandas as pd
from config import (
    PROJECT_ROOT,
    NOAA_TOKEN,
    NOAA_BASE_URL,
    NOAA_DATASET_ID,
    BASELINE_OUTPUT_FILE,
    ANALYSIS_OUTPUT_FILE,
    ANALYSIS_END_DATE,
)

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

MAPPING_FILE = DATA_RAW / "parish_station_mapping.csv"

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


def get_station_weather(station_id, start_date, end_date, datatype_ids):
    rows = get_all_pages(
        "data",
        {
            "datasetid": NOAA_DATASET_ID,
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "units": "standard",
            "datatypeid": datatype_ids,
        },
    )
    return pd.DataFrame(rows)


def build_baseline_dataset(mapping_df, datatype_ids):
    frames = []

    for _, row in mapping_df.iterrows():
        station_id = row["station_id"]
        parish = row["parish"]
        station_name = row["station_name"]

        print(f"Building baseline data for {parish} -> {station_name}")

        for year in range(1981, 2011):
            df = get_station_weather(
                station_id=station_id,
                start_date=f"{year}-07-01",
                end_date=f"{year}-08-31",
                datatype_ids=datatype_ids,
            )

            if not df.empty:
                df["parish"] = parish
                df["station_id"] = station_id
                df["station_name"] = station_name
                frames.append(df)

    if frames:
        baseline = pd.concat(frames, ignore_index=True)
    else:
        baseline = pd.DataFrame()

    output_file = DATA_RAW / BASELINE_OUTPUT_FILE
    baseline.to_csv(output_file, index=False)

    return baseline


def build_analysis_dataset(mapping_df, datatype_ids):
    frames = []

    for _, row in mapping_df.iterrows():
        station_id = row["station_id"]
        parish = row["parish"]
        station_name = row["station_name"]

        print(f"Building analysis data for {parish} -> {station_name}")

        for year in range(2015, 2026):
            end_date = f"{year}-12-31"
            if year == 2025:
                end_date = ANALYSIS_END_DATE

            df = get_station_weather(
                station_id=station_id,
                start_date=f"{year}-01-01",
                end_date=end_date,
                datatype_ids=datatype_ids,
            )

            if not df.empty:
                df["parish"] = parish
                df["station_id"] = station_id
                df["station_name"] = station_name
                frames.append(df)

    if frames:
        analysis = pd.concat(frames, ignore_index=True)
    else:
        analysis = pd.DataFrame()

    output_file = DATA_RAW / ANALYSIS_OUTPUT_FILE
    analysis.to_csv(output_file, index=False)

    return analysis


def main():
    mapping_df = pd.read_csv(MAPPING_FILE)

    datatype_ids = ["TMIN", "TMAX", "TAVG"]

    baseline = build_baseline_dataset(mapping_df, datatype_ids)
    print(f"\nBaseline rows saved: {len(baseline)}")

    analysis = build_analysis_dataset(mapping_df, datatype_ids)
    print(f"Analysis rows saved: {len(analysis)}")


if __name__ == "__main__":
    main()