import pandas as pd
import geopandas as gpd
from config import PROJECT_ROOT

DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_RAW = PROJECT_ROOT / "data" / "raw"

INPUT_FILE = DATA_PROCESSED / "lcd_analysis_2015_2025_daily.csv"
OUTPUT_FILE = DATA_PROCESSED / "lcd_analysis_2015_2025_daily_filled.csv"

PARISH_SHAPE_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip"
)


def build_parish_distance_lookup():
    """
    Build a nearest-parish lookup using representative points.
    """
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"].copy()
    parishes = parishes[["NAME", "geometry"]].copy()

    parish_points = parishes.copy()
    parish_points["geometry"] = parish_points.representative_point()

    parish_points = parish_points.to_crs("EPSG:3857")

    rows = []
    for i, row_i in parish_points.iterrows():
        for j, row_j in parish_points.iterrows():
            if row_i["NAME"] == row_j["NAME"]:
                continue

            distance_km = row_i.geometry.distance(row_j.geometry) / 1000
            rows.append(
                {
                    "parish": row_i["NAME"],
                    "nearby_parish": row_j["NAME"],
                    "distance_km": distance_km,
                }
            )

    distance_df = pd.DataFrame(rows)
    distance_df = distance_df.sort_values(["parish", "distance_km"]).reset_index(
        drop=True
    )

    return distance_df


def fill_variable_from_nearby(df, distance_df, variable):
    """
    Fill one variable using the nearest nearby parish value on the same date.
    """
    filled_df = df.copy()

    missing_mask = filled_df[variable].isna()
    missing_rows = filled_df.loc[missing_mask, ["parish", "date"]].copy()

    if missing_rows.empty:
        return filled_df, 0

    fill_count = 0

    for idx, missing_row in missing_rows.iterrows():
        parish = missing_row["parish"]
        date = missing_row["date"]

        nearby_options = distance_df[distance_df["parish"] == parish]

        for _, option in nearby_options.iterrows():
            nearby_parish = option["nearby_parish"]

            match = filled_df[
                (filled_df["parish"] == nearby_parish)
                & (filled_df["date"] == date)
                & (filled_df[variable].notna())
            ]

            if not match.empty:
                filled_value = match.iloc[0][variable]
                filled_df.loc[
                    (filled_df["parish"] == parish)
                    & (filled_df["date"] == date)
                    & (filled_df[variable].isna()),
                    variable,
                ] = filled_value
                fill_count += 1
                break

    return filled_df, fill_count


def main():
    df = pd.read_csv(INPUT_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    weather_cols = [
        "avg_temp",
        "avg_dew_point",
        "avg_wind_speed",
        "min_temp",
        "max_temp",
    ]

    # Ensure numeric columns are numeric
    for col in weather_cols + ["avg_relative_humidity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("Missing values before fill:")
    print(df[weather_cols].isna().sum())

    distance_df = build_parish_distance_lookup()

    total_fill_counts = {}

    for variable in weather_cols:
        df, fill_count = fill_variable_from_nearby(df, distance_df, variable)
        total_fill_counts[variable] = fill_count

    print("\nFilled counts:")
    for variable, count in total_fill_counts.items():
        print(f"{variable}: {count}")

    print("\nMissing values after fill:")
    print(df[weather_cols].isna().sum())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved filled LCD dataset to:\n{OUTPUT_FILE}")


if __name__ == "__main__":
    main()