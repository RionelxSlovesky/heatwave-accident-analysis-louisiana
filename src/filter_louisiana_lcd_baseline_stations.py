import pandas as pd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"

INPUT_FILE = DATA_RAW / "louisiana_lcd_stations.csv"
OUTPUT_FILE = DATA_RAW / "louisiana_lcd_baseline_candidate_stations.csv"


def main():
    df = pd.read_csv(INPUT_FILE)

    df["mindate"] = pd.to_datetime(df["mindate"], errors="coerce")
    df["maxdate"] = pd.to_datetime(df["maxdate"], errors="coerce")

    baseline_candidates = df[
        (df["latitude"].notna())
        & (df["longitude"].notna())
        & (df["mindate"] <= pd.Timestamp("1981-07-01"))
        & (df["maxdate"] >= pd.Timestamp("2010-08-31"))
        & (df["datacoverage"] >= 0.8)
    ].copy()

    baseline_candidates = baseline_candidates.sort_values(
        ["datacoverage", "mindate"], ascending=[False, True]
    )

    baseline_candidates.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved LCD baseline candidate stations to: {OUTPUT_FILE}")
    print(f"Baseline candidate count: {len(baseline_candidates)}")
    print("\nPreview:")
    print(
        baseline_candidates[
            [
                "id",
                "name",
                "latitude",
                "longitude",
                "mindate",
                "maxdate",
                "datacoverage",
            ]
        ].head(20)
    )


if __name__ == "__main__":
    main()