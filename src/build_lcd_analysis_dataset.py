import pandas as pd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

MAPPING_FILE = DATA_RAW / "parish_lcd_station_mapping.csv"
OUTPUT_FILE = DATA_PROCESSED / "lcd_analysis_2015_2025_daily.csv"

BASE_URL = "https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access"


def wban_to_lcd_ghcn_id(wban_id):
    digits = str(wban_id).split(":")[-1].zfill(5)
    return f"USW000{digits}"


def clean_numeric_series(series):
    """
    Extract numeric values from NOAA LCD hourly columns.
    Handles values like:
    '77', '77.0', '77s', '0000', 'VRB', etc.
    Keeps leading minus signs and decimals where present.
    """
    return pd.to_numeric(
        series.astype(str).str.extract(r"(-?\d+\.?\d*)", expand=False), errors="coerce"
    )


def download_station_year(lcd_ghcn_id, year):
    url = f"{BASE_URL}/{year}/LCD_{lcd_ghcn_id}_{year}.csv"
    print(f"Downloading: {url}")

    try:
        return pd.read_csv(url, low_memory=False)
    except Exception as e:
        print(f"Failed for {lcd_ghcn_id}, {year}: {e}")
        return pd.DataFrame()


def reduce_hourly_to_daily(
    df, parish, lcd_station_id, lcd_station_name, station_latitude, station_longitude
):
    if df.empty:
        return pd.DataFrame()

    # Parse datetime and keep only analysis-period rows
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df[
        (df["DATE"] >= pd.Timestamp("2015-01-01"))
        & (df["DATE"] <= pd.Timestamp("2025-07-31 23:59:59"))
    ].copy()

    if df.empty:
        return pd.DataFrame()

    # Clean hourly columns to numeric
    hourly_cols = [
        "HourlyDryBulbTemperature",
        "HourlyDewPointTemperature",
        "HourlyRelativeHumidity",
        "HourlyWindSpeed",
    ]

    available_hourly_cols = [col for col in hourly_cols if col in df.columns]
    if not available_hourly_cols:
        return pd.DataFrame()

    for col in available_hourly_cols:
        df[col] = clean_numeric_series(df[col])

    df["date"] = df["DATE"].dt.date

    agg_dict = {}

    if "HourlyDryBulbTemperature" in df.columns:
        agg_dict["HourlyDryBulbTemperature"] = ["mean", "min", "max"]

    if "HourlyDewPointTemperature" in df.columns:
        agg_dict["HourlyDewPointTemperature"] = ["mean"]

    if "HourlyRelativeHumidity" in df.columns:
        agg_dict["HourlyRelativeHumidity"] = ["mean"]

    if "HourlyWindSpeed" in df.columns:
        agg_dict["HourlyWindSpeed"] = ["mean"]

    daily_df = df.groupby("date").agg(agg_dict)

    # Flatten multi-index columns
    daily_df.columns = [
        "_".join(col).strip("_") for col in daily_df.columns.to_flat_index()
    ]
    daily_df = daily_df.reset_index()

    # Rename into clean analysis names
    rename_map = {
        "HourlyDryBulbTemperature_mean": "avg_temp",
        "HourlyDryBulbTemperature_min": "min_temp",
        "HourlyDryBulbTemperature_max": "max_temp",
        "HourlyDewPointTemperature_mean": "avg_dew_point",
        "HourlyRelativeHumidity_mean": "avg_relative_humidity",
        "HourlyWindSpeed_mean": "avg_wind_speed",
    }
    daily_df = daily_df.rename(columns=rename_map)

    # Add parish/station metadata
    daily_df["parish"] = parish
    daily_df["lcd_station_id"] = lcd_station_id
    daily_df["lcd_station_name"] = lcd_station_name
    daily_df["station_latitude"] = station_latitude
    daily_df["station_longitude"] = station_longitude

    daily_df["date"] = pd.to_datetime(daily_df["date"])

    return daily_df


def main():
    mapping_df = pd.read_csv(MAPPING_FILE).copy()
    mapping_df["lcd_ghcn_id"] = mapping_df["lcd_station_id"].apply(wban_to_lcd_ghcn_id)

    mapping_df = mapping_df[
        [
            "parish",
            "lcd_station_id",
            "lcd_station_name",
            "station_latitude",
            "station_longitude",
            "lcd_ghcn_id",
        ]
    ].drop_duplicates()

    reduced_frames = []

    for _, row in mapping_df.iterrows():
        parish = row["parish"]
        lcd_station_id = row["lcd_station_id"]
        lcd_station_name = row["lcd_station_name"]
        station_latitude = row["station_latitude"]
        station_longitude = row["station_longitude"]
        lcd_ghcn_id = row["lcd_ghcn_id"]

        print(
            f"\nBuilding LCD hourly-derived daily data for {parish} -> {lcd_station_name}"
        )

        for year in range(2015, 2026):
            raw_df = download_station_year(lcd_ghcn_id, year)

            daily_df = reduce_hourly_to_daily(
                raw_df,
                parish=parish,
                lcd_station_id=lcd_station_id,
                lcd_station_name=lcd_station_name,
                station_latitude=station_latitude,
                station_longitude=station_longitude,
            )

            if not daily_df.empty:
                reduced_frames.append(daily_df)

    if not reduced_frames:
        print("No LCD hourly-derived daily data created.")
        return

    final_df = pd.concat(reduced_frames, ignore_index=True)

    # Keep a clean column order
    wanted_cols = [
        "parish",
        "lcd_station_id",
        "lcd_station_name",
        "station_latitude",
        "station_longitude",
        "date",
        "avg_temp",
        "avg_dew_point",
        "avg_relative_humidity",
        "avg_wind_speed",
        "max_temp",
        "min_temp",
    ]
    final_df = final_df[[col for col in wanted_cols if col in final_df.columns]]

    final_df = final_df.sort_values(["parish", "date"]).reset_index(drop=True)

    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved LCD analysis dataset to: {OUTPUT_FILE}")
    print(f"Rows saved: {len(final_df):,}")
    print("\nColumns:")
    print(final_df.columns.tolist())
    print("\nPreview:")
    print(final_df.head())


if __name__ == "__main__":
    main()