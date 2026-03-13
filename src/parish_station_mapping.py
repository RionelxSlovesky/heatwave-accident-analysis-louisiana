import pandas as pd
import geopandas as gpd
from pathlib import Path
from config import PROJECT_ROOT, PARISH_SHAPE_URL

# Paths
DATA_RAW = PROJECT_ROOT / "data" / "raw"
OUTPUT_FILE = DATA_RAW / "parish_station_mapping.csv"
STATIONS_FILE = DATA_RAW / "louisiana_candidate_stations.csv"


def main():
    # Load candidate stations
    stations = pd.read_csv(STATIONS_FILE)

    # Convert candidate stations to GeoDataFrame
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations["longitude"], stations["latitude"]),
        crs="EPSG:4326",
    )

    # Load Louisiana parish boundaries
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"].copy()
    parishes = parishes[["NAME", "NAMELSAD", "geometry"]]

    # Use representative points for parish location
    parish_points = parishes.copy()
    parish_points["geometry"] = parish_points.representative_point()

    # Reproject both layers before distance calculation
    parish_points_proj = parish_points.to_crs("EPSG:3857")
    stations_proj = stations_gdf.to_crs("EPSG:3857")

    # Find nearest station for each parish
    mapping = gpd.sjoin_nearest(
        parish_points_proj, stations_proj, how="left", distance_col="distance_m"
    )

    # Convert distance to kilometers
    mapping["distance_km"] = mapping["distance_m"] / 1000

    # Keep useful columns
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
            "id": "station_id",
            "name": "station_name",
            "latitude": "station_latitude",
            "longitude": "station_longitude",
        }
    )

    # Save output
    parish_station_mapping.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved parish-station mapping to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(parish_station_mapping.head())
    print(f"\nNumber of parishes mapped: {len(parish_station_mapping)}")
    print(f"Unique parishes: {parish_station_mapping['parish'].nunique()}")
    print(f"Unique stations used: {parish_station_mapping['station_id'].nunique()}")


if __name__ == "__main__":
    main()