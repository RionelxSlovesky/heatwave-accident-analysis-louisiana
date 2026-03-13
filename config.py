from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

NOAA_TOKEN = os.getenv("NOAA_TOKEN")
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

BASELINE_OUTPUT_FILE = "weather_baseline_1981_2010_jul_aug.csv"
ANALYSIS_OUTPUT_FILE = "weather_analysis_2015_2025_all_months.csv"

BASELINE_START_DATE = "1981-07-01"
BASELINE_END_DATE = "2010-08-31"

ANALYSIS_START_DATE = "2015-01-01"
ANALYSIS_END_DATE = "2025-07-31"

NOAA_DATASET_ID = "GHCND"
LOUISIANA_LOCATION_ID = "FIPS:22"