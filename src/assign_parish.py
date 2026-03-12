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

    # 3) Standardize city and zip for fallback mapping
    df["city"] = df["city"].astype(str).str.strip().str.upper()
    df["Zip"] = df["Zip"].astype(str).str.extract(r"(\d{5})", expand=False)

    # 4) Convert accident rows to geographic points
    accidents_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"], df["Latitude"]),
        crs="EPSG:4326",
    )

    # 5) Load parish boundaries and keep only Louisiana
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"][
        ["NAME", "NAMELSAD", "geometry"]
    ].copy()

    # 6) Make CRS match before spatial join
    parishes = parishes.to_crs(accidents_gdf.crs)

    # 7) Spatial join: assign parish to each accident point
    joined = gpd.sjoin(accidents_gdf, parishes, how="left", predicate="within")

    # 8) Create parish column from spatial join result
    joined["parish"] = joined["NAME"]

    # 9) Fallback map for rows with bad/missing coordinates
    fallback_map = {
        ("METAIRIE", "70001"): "Jefferson",
        ("MANSFIELD", "71052"): "De Soto",
        ("DEQUINCY", "70633"): "Calcasieu",
        ("WALKER", "70785"): "Livingston",
        ("LONGSTREET", "71050"): "De Soto",
        ("LAFAYETTE", "70506"): "Lafayette",
        ("LAKE CHARLES", "70605"): "Calcasieu",
        ("PONCHATOULA", "70454"): "Tangipahoa",
        ("NEW ORLEANS", "70118"): "Orleans",
        ("STONEWALL", "71078"): "De Soto",
        ("CHALMETTE", "70043"): "St. Bernard",
        ("ELM GROVE", "71051"): "Bossier",
        ("BATON ROUGE", "70806"): "East Baton Rouge",
        ("METAIRIE", "70003"): "Jefferson",
    }

    mask = joined["parish"].isna()
    joined.loc[mask, "parish"] = joined.loc[mask].apply(
        lambda row: fallback_map.get((row["city"], row["Zip"]), row["parish"]), axis=1
    )

    # 10) Drop geometry and extra join columns before saving
    drop_cols = ["geometry", "index_right", "NAME", "NAMELSAD"]
    joined = joined.drop(columns=[col for col in drop_cols if col in joined.columns])

    # 11) Save processed file
    joined.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved processed file with parish column to:\n{OUTPUT_FILE}")
    print("\nMissing parish values after fallback:", joined["parish"].isna().sum())
    print("\nPreview:")
    print(
        joined[["EventDate", "city", "Zip", "Latitude", "Longitude", "parish"]].head()
    )


if __name__ == "__main__":
    main()