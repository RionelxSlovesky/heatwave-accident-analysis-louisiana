import pandas as pd
from pathlib import Path


# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_PROCESSED / "LouisianaAccidentData_with_parish.csv"
OUTPUT_FILE = DATA_PROCESSED / "daily_accident_counts_by_parish.csv"


def main():
    # Read processed accident data with parish
    df = pd.read_csv(INPUT_FILE)

    # Parse date
    df["EventDate"] = pd.to_datetime(df["EventDate"], errors="coerce")

    # Convert datetime to date only
    df["date"] = df["EventDate"].dt.date

    # Aggregate daily counts by parish
    daily_counts = (
        df.groupby(["date", "parish"])
        .size()
        .reset_index(name="accident_count")
        .sort_values(["date", "parish"])
    )

    # Save output
    daily_counts.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved aggregated daily counts to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(daily_counts.head(10))
    print(f"\nTotal aggregated rows: {len(daily_counts):,}")
    print(f"Total accidents counted: {daily_counts['accident_count'].sum():,}")


if __name__ == "__main__":
    main()