import pandas as pd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

# NOAA GHCNh station list
STATION_LIST_URL = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/"
    "ghcnh-station-list.csv"
)

OUTPUT_FILE = DATA_RAW / "louisiana_ghcnh_stations.csv"


def main():
    # Load full GHCNh station list
    stations = pd.read_csv(STATION_LIST_URL)

    print("Columns in source file:")
    print(stations.columns.tolist())

    # Try to identify the state column robustly
    state_col = None
    for col in stations.columns:
        if col.lower() in ["state", "st", "state_code"]:
            state_col = col
            break

    if state_col is None:
        raise ValueError("Could not find a state column in the GHCNh station list.")

    # Filter Louisiana stations
    louisiana_stations = stations[
        stations[state_col].astype(str).str.upper() == "LA"
    ].copy()

    louisiana_stations.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved Louisiana GHCNh stations to: {OUTPUT_FILE}")
    print(f"Total Louisiana GHCNh stations: {len(louisiana_stations)}")
    print("\nPreview:")
    print(louisiana_stations.head())


if __name__ == "__main__":
    main()