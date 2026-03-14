import pandas as pd
import geopandas as gpd
from config import PROJECT_ROOT

# Paths
DATA_RAW = PROJECT_ROOT / "data" / "raw"

STATIONS_FILE = DATA_RAW / "louisiana_ghcnh_candidate_stations.csv"
OUTPUT_FILE = DATA_RAW / "parish_ghcnh_station_mapping.csv"

PARISH_SHAPE_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2025/COUNTY/tl_2025_us_county.zip"
)


def main():
    # Load GHCNh candidate stations
    stations = pd.read_csv(STATIONS_FILE)

    # Convert stations to GeoDataFrame
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations["LONGITUDE"], stations["LATITUDE"]),
        crs="EPSG:4326",
    )

    # Load Louisiana parish boundaries
    parishes = gpd.read_file(PARISH_SHAPE_URL)
    parishes = parishes[parishes["STATEFP"] == "22"].copy()
    parishes = parishes[["NAME", "NAMELSAD", "geometry"]]

    # Use representative points instead of centroids
    parish_points = parishes.copy()
    parish_points["geometry"] = parish_points.representative_point()

    # Reproject before distance calculation
    parish_points_proj = parish_points.to_crs("EPSG:3857")
    stations_proj = stations_gdf.to_crs("EPSG:3857")

    # Find nearest GHCNh station for each parish
    mapping = gpd.sjoin_nearest(
        parish_points_proj, stations_proj, how="left", distance_col="distance_m"
    )

    # Convert distance to kilometers
    mapping["distance_km"] = mapping["distance_m"] / 1000

    # Keep useful columns
    parish_station_mapping = mapping[
        [
            "NAME_left",
            "NAMELSAD",
            "GHCN_ID",
            "NAME_right",
            "LATITUDE",
            "LONGITUDE",
            "ELEVATION",
            "distance_km",
        ]
    ].copy()

    # Rename columns clearly
    parish_station_mapping = parish_station_mapping.rename(
        columns={
            "NAME_left": "parish",
            "NAMELSAD": "parish_full",
            "GHCN_ID": "ghcnh_station_id",
            "NAME_right": "ghcnh_station_name",
            "LATITUDE": "station_latitude",
            "LONGITUDE": "station_longitude",
            "ELEVATION": "station_elevation",
        }
    )

    # Save output
    parish_station_mapping.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved parish GHCNh mapping to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(parish_station_mapping.head())
    print(f"\nNumber of parishes mapped: {len(parish_station_mapping)}")
    print(f"Unique parishes: {parish_station_mapping['parish'].nunique()}")
    print(
        f"Unique GHCNh stations used: {parish_station_mapping['ghcnh_station_id'].nunique()}"
    )


if __name__ == "__main__":
    main()
