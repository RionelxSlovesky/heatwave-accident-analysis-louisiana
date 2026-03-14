import numpy as np
import pandas as pd
import geopandas as gpd
from config import PROJECT_ROOT

DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_PROCESSED / "lcd_baseline_1981_2010_jul_aug_daily.csv"
OUTPUT_FILE = DATA_PROCESSED / "lcd_baseline_1981_2010_jul_aug_daily_filled.csv"

PARISH_SHAPE_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip"
)


def build_parish_distance_lookup():
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"][["NAME", "geometry"]].copy()

    parish_points = parishes.copy()
    parish_points["geometry"] = parish_points.representative_point()
    parish_points = parish_points.to_crs("EPSG:3857")

    rows = []
    for _, row_i in parish_points.iterrows():
        for _, row_j in parish_points.iterrows():
            if row_i["NAME"] == row_j["NAME"]:
                continue
            rows.append(
                {
                    "parish": row_i["NAME"],
                    "nearby_parish": row_j["NAME"],
                    "distance_km": row_i.geometry.distance(row_j.geometry) / 1000,
                }
            )

    distance_df = (
        pd.DataFrame(rows).sort_values(["parish", "distance_km"]).reset_index(drop=True)
    )
    return distance_df


def fill_variable_from_nearby(df, distance_df, variable):
    filled_df = df.copy()
    missing_rows = filled_df.loc[filled_df[variable].isna(), ["parish", "date"]].copy()

    fill_count = 0

    for _, missing_row in missing_rows.iterrows():
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
                filled_df.loc[
                    (filled_df["parish"] == parish)
                    & (filled_df["date"] == date)
                    & (filled_df[variable].isna()),
                    variable,
                ] = match.iloc[0][variable]
                fill_count += 1
                break

    return filled_df, fill_count


def f_to_c(temp_f):
    return (temp_f - 32) * 5 / 9


def mph_to_ms(speed_mph):
    return speed_mph * 0.44704


def vapor_pressure_from_dewpoint(td_c):
    return 6.105 * np.exp((17.27 * td_c) / (237.7 + td_c))


def apparent_temperature(temp_c, vapor_pressure_hpa, wind_ms):
    return temp_c + 0.33 * vapor_pressure_hpa - 0.70 * wind_ms - 4.00


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

    for col in weather_cols + ["avg_relative_humidity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print("Missing before fill:")
    print(df[weather_cols].isna().sum())

    distance_df = build_parish_distance_lookup()

    fill_counts = {}
    for variable in weather_cols:
        df, fill_count = fill_variable_from_nearby(df, distance_df, variable)
        fill_counts[variable] = fill_count

    print("\nFilled counts:")
    for variable, count in fill_counts.items():
        print(f"{variable}: {count}")

    print("\nMissing after fill:")
    print(df[weather_cols].isna().sum())

    # Apparent temperature inputs
    df["avg_temp_c"] = f_to_c(df["avg_temp"])
    df["min_temp_c"] = f_to_c(df["min_temp"])
    df["avg_dew_point_c"] = f_to_c(df["avg_dew_point"])
    df["avg_wind_speed_ms"] = mph_to_ms(df["avg_wind_speed"])

    # Vapor pressure from dew point
    df["vapor_pressure_hpa"] = vapor_pressure_from_dewpoint(df["avg_dew_point_c"])

    # Apparent temperature
    df["avg_apparent_temp_c"] = apparent_temperature(
        df["avg_temp_c"],
        df["vapor_pressure_hpa"],
        df["avg_wind_speed_ms"],
    )

    df["min_apparent_temp_c"] = apparent_temperature(
        df["min_temp_c"],
        df["vapor_pressure_hpa"],
        df["avg_wind_speed_ms"],
    )

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved prepared baseline file to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(
        df[
            [
                "parish",
                "date",
                "avg_temp",
                "min_temp",
                "avg_dew_point",
                "avg_wind_speed",
                "avg_apparent_temp_c",
                "min_apparent_temp_c",
            ]
        ].head()
    )


if __name__ == "__main__":
    main()