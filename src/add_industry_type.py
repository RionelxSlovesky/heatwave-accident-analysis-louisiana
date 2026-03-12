import pandas as pd
from pathlib import Path
from naics import industry

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

INPUT_FILE = DATA_PROCESSED / "LouisianaAccidentData_with_parish.csv"
OUTPUT_FILE = DATA_PROCESSED / "LouisianaAccidentData_with_parish.csv"

# Read the processed accident dataset
df = pd.read_csv(INPUT_FILE)


def classify_naics_2digit(value):
    """
    Convert the Primary NAICS code into a broad 2-digit industry type.
    Special grouped NAICS sectors are handled manually:
    31-33 -> Manufacturing
    44-45 -> Retail Trade
    48-49 -> Transportation and Warehousing
    All other 2-digit codes are classified using the naics package.
    """
    try:
        # Take only the first 2 digits from the NAICS code
        code = str(value).strip()[:2]

        # Handle grouped NAICS sectors manually
        if code in ["31", "32", "33"]:
            return "Manufacturing"
        if code in ["44", "45"]:
            return "Retail Trade"
        if code in ["48", "49"]:
            return "Transportation and Warehousing"
        if code == "99":
            return "Unknown"

        # For all other sectors, use the naics classification tool
        return str(industry(int(code)))
    except:
        # Return None if the value is missing or invalid
        return None


# Create a new column with the extracted 2-digit NAICS code
df["naics_2digit"] = df["Primary_NAICS"].astype(str).str.strip().str[:2]

# Create a new column with the corresponding industry type
df["industry_type"] = df["Primary_NAICS"].apply(classify_naics_2digit)

# Save the updated dataset back to the same file
df.to_csv(OUTPUT_FILE, index=False)

# Preview the result
print(df[["Primary_NAICS", "naics_2digit", "industry_type"]].head())