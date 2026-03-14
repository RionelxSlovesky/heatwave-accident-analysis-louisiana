import pandas as pd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"

INPUT_FILE = DATA_RAW / "louisiana_lcd_stations.csv"
OUTPUT_FILE = DATA_RAW / "louisiana_lcd_candidate_stations.csv"


def main():
    df = pd.read_csv(INPUT_FILE)

    df["mindate"] = pd.to_datetime(df["mindate"], errors="coerce")
    df["maxdate"] = pd.to_datetime(df["maxdate"], errors="coerce")

    candidate_stations = df[
        (df["latitude"].notna())
        & (df["longitude"].notna())
        & (df["mindate"] <= pd.Timestamp("2015-01-01"))
        & (df["maxdate"] >= pd.Timestamp("2025-07-31"))
        & (df["datacoverage"] >= 0.8)
    ].copy()

    candidate_stations = candidate_stations.sort_values("datacoverage", ascending=False)

    candidate_stations.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved LCD candidate stations to: {OUTPUT_FILE}")
    print(f"Candidate station count: {len(candidate_stations)}")
    print("\nPreview:")
    print(
        candidate_stations[
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