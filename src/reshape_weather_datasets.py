import pandas as pd
from config import PROJECT_ROOT, BASELINE_OUTPUT_FILE, ANALYSIS_OUTPUT_FILE

DATA_RAW = PROJECT_ROOT / "data" / "raw"

BASELINE_INPUT = DATA_RAW / BASELINE_OUTPUT_FILE
ANALYSIS_INPUT = DATA_RAW / ANALYSIS_OUTPUT_FILE

BASELINE_OUTPUT = BASELINE_INPUT
ANALYSIS_OUTPUT = ANALYSIS_INPUT


def reshape_weather_file(input_file, output_file):
    df = pd.read_csv(input_file)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    wide_df = df.pivot_table(
        index=["parish", "station_id", "station_name", "date"],
        columns="datatype",
        values="value",
        aggfunc="first",
    ).reset_index()

    wide_df.columns.name = None

    wide_df.to_csv(output_file, index=False)

    print(f"Saved reshaped dataset to: {output_file}")
    print("\nColumns:")
    print(wide_df.columns.tolist())
    print("\nPreview:")
    print(wide_df.head())


def main():
    reshape_weather_file(BASELINE_INPUT, BASELINE_OUTPUT)
    reshape_weather_file(ANALYSIS_INPUT, ANALYSIS_OUTPUT)


if __name__ == "__main__":
    main()
