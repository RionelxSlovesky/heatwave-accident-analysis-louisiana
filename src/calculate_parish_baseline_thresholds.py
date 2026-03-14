import pandas as pd
from config import PROJECT_ROOT

DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_PROCESSED / "lcd_baseline_1981_2010_jul_aug_daily_filled.csv"
OUTPUT_FILE = DATA_PROCESSED / "parish_baseline_apparent_temp_thresholds.csv"


def main():
    df = pd.read_csv(INPUT_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["min_apparent_temp_c"] = pd.to_numeric(
        df["min_apparent_temp_c"], errors="coerce"
    )

    # keep only valid rows
    df = df.dropna(subset=["parish", "min_apparent_temp_c"]).copy()

    # calculate parish-specific 85th percentile threshold
    thresholds = (
        df.groupby("parish")["min_apparent_temp_c"]
        .quantile(0.85)
        .reset_index(name="apparent_temp_85th_percentile")
        .sort_values("parish")
        .reset_index(drop=True)
    )

    thresholds.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved parish baseline thresholds to:\n{OUTPUT_FILE}")
    print(f"\nNumber of parishes: {len(thresholds)}")
    print("\nPreview:")
    print(thresholds.head())


if __name__ == "__main__":
    main()