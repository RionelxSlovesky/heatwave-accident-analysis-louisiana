import pandas as pd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"

INPUT_FILE = DATA_RAW / "louisiana_ghcnh_stations.csv"
OUTPUT_FILE = DATA_RAW / "louisiana_ghcnh_candidate_stations.csv"


def main():
    df = pd.read_csv(INPUT_FILE)

    # keep only rows with valid coordinates
    df = df[df["LATITUDE"].notna() & df["LONGITUDE"].notna()].copy()

    # remove offshore/marine stations
    exclude_terms = [
        "GREEN CANYON",
        "EUGENE ISLAND",
        "GULF",
        "BUOY",
        "OIL PLATFORM",
        "RIG",
        "SHIP",
        "LAKE PONTCHARTRAIN",
    ]

    pattern = "|".join(exclude_terms)
    df = df[~df["NAME"].astype(str).str.upper().str.contains(pattern, na=False)].copy()

    # sort for easier review
    df = df.sort_values("NAME").reset_index(drop=True)

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved filtered GHCNh candidate stations to: {OUTPUT_FILE}")
    print(f"Candidate station count: {len(df)}")
    print("\nPreview:")
    print(df[["GHCN_ID", "NAME", "LATITUDE", "LONGITUDE", "STATE"]].head(20))


if __name__ == "__main__":
    main()