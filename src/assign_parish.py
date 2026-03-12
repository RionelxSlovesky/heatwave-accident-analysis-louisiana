import pandas as pd
import geopandas as gpd
from pathlib import Path


# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

ACCIDENT_FILE = DATA_RAW / "LouisianaAccidentData.csv"
OUTPUT_FILE = DATA_PROCESSED / "LouisianaAccidentData_with_parish.csv"
PARISH_SHAPE_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip"
)


def main():
    # 1) Read Louisiana accident data
    df = pd.read_csv(ACCIDENT_FILE, engine="python", on_bad_lines="skip")

    # 2) Clean date and coordinates
    df["EventDate"] = pd.to_datetime(df["EventDate"], errors="coerce")
    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

    # 3) Convert accident rows to geographic points
    accidents_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326",
    )

    # 4) Load parish boundaries and keep only Louisiana
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"][
        ["NAME", "NAMELSAD", "geometry"]
    ].copy()

    # 5) Spatial join: assign parish to each accident point
    joined = gpd.sjoin(accidents_gdf, parishes, how="left", predicate="within")

    # 6) Add parish column
    joined["parish"] = joined["NAME"]

    # 7) Drop geometry and extra join columns before saving
    drop_cols = ["geometry", "index_right", "NAME", "NAMELSAD"]
    joined = joined.drop(columns=[col for col in drop_cols if col in joined.columns])

    # 8) Save processed file
    joined.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved processed file with parish column to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(
        joined[["EventDate", "City", "Zip", "Latitude", "Longitude", "parish"]].head()
    )


if __name__ == "__main__":
    main()