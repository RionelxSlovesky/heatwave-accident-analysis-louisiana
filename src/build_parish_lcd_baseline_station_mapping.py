import pandas as pd
import geopandas as gpd
from config import PROJECT_ROOT

DATA_RAW = PROJECT_ROOT / "data" / "raw"

STATIONS_FILE = DATA_RAW / "louisiana_lcd_baseline_candidate_stations.csv"
OUTPUT_FILE = DATA_RAW / "parish_lcd_baseline_station_mapping.csv"

PARISH_SHAPE_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip"
)


def main():
    stations = pd.read_csv(STATIONS_FILE)

    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations["longitude"], stations["latitude"]),
        crs="EPSG:4326",
    )

    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"].copy()
    parishes = parishes[["NAME", "NAMELSAD", "geometry"]]

    parish_points = parishes.copy()
    parish_points["geometry"] = parish_points.representative_point()

    parish_points_proj = parish_points.to_crs("EPSG:3857")
    stations_proj = stations_gdf.to_crs("EPSG:3857")

    mapping = gpd.sjoin_nearest(
        parish_points_proj, stations_proj, how="left", distance_col="distance_m"
    )

    mapping["distance_km"] = mapping["distance_m"] / 1000

    parish_station_mapping = mapping[
        [
            "NAME",
            "NAMELSAD",
            "id",
            "name",
            "latitude",
            "longitude",
            "datacoverage",
            "distance_km",
        ]
    ].copy()

    parish_station_mapping = parish_station_mapping.rename(
        columns={
            "NAME": "parish",
            "NAMELSAD": "parish_full",
            "id": "lcd_station_id",
            "name": "lcd_station_name",
            "latitude": "station_latitude",
            "longitude": "station_longitude",
        }
    )

    parish_station_mapping.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved parish LCD baseline mapping to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(parish_station_mapping.head())
    print(f"\nParishes mapped: {len(parish_station_mapping)}")
    print(
        f"Unique baseline LCD stations used: {parish_station_mapping['lcd_station_id'].nunique()}"
    )


if __name__ == "__main__":
    main()