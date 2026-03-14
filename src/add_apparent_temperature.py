import numpy as np
import pandas as pd
from config import PROJECT_ROOT

DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_PROCESSED / "lcd_analysis_2015_2025_daily_filled.csv"
OUTPUT_FILE = INPUT_FILE  # overwrite the same file


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

    weather_cols = ["avg_temp", "min_temp", "avg_dew_point", "avg_wind_speed"]
    for col in weather_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert from Fahrenheit to Celsius
    df["avg_temp_c"] = f_to_c(df["avg_temp"])
    df["min_temp_c"] = f_to_c(df["min_temp"])
    df["avg_dew_point_c"] = f_to_c(df["avg_dew_point"])

    # Convert wind speed from mph to m/s
    df["avg_wind_speed_ms"] = mph_to_ms(df["avg_wind_speed"])

    # Compute vapor pressure from dew point
    df["vapor_pressure_hpa"] = vapor_pressure_from_dewpoint(df["avg_dew_point_c"])

    # Compute apparent temperature
    df["avg_apparent_temp_c"] = apparent_temperature(
        df["avg_temp_c"],
        df["vapor_pressure_hpa"],
        df["avg_wind_speed_ms"],
    )

    # Approximate daily minimum apparent temperature
    df["min_apparent_temp_c"] = apparent_temperature(
        df["min_temp_c"],
        df["vapor_pressure_hpa"],
        df["avg_wind_speed_ms"],
    )

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Updated file saved to:\n{OUTPUT_FILE}")
    print("\nNew columns added:")
    print(
        [
            "avg_temp_c",
            "min_temp_c",
            "avg_dew_point_c",
            "avg_wind_speed_ms",
            "vapor_pressure_hpa",
            "avg_apparent_temp_c",
            "min_apparent_temp_c",
        ]
    )
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