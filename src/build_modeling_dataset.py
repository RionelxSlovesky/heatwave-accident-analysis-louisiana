import pandas as pd
from config import PROJECT_ROOT

DATA_FINAL = PROJECT_ROOT / "data" / "final"

MERGED_FILE = DATA_FINAL / "merged_parish_day_analysis.csv"
COVARIATE_FILE = DATA_FINAL / "parish_covariates.csv"
OUTPUT_FILE = DATA_FINAL / "parish_day_modeling_dataset.csv"


def main():
    merged = pd.read_csv(MERGED_FILE)
    covariates = pd.read_csv(COVARIATE_FILE)

    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")

    # Standardize parish names
    merged["parish"] = merged["parish"].astype(str).str.strip()
    covariates["parish"] = covariates["parish"].astype(str).str.strip()

    # Merge parish-level covariates onto parish-day data
    model_df = merged.merge(covariates, on="parish", how="left")

    # Create binary heat-wave indicator for modeling
    if "heatwave_flag" in model_df.columns:
        model_df["heatwave_binary"] = (model_df["heatwave_flag"] == "heat-wave").astype(
            int
        )

    # Convert common covariate fields to numeric if present
    numeric_cols = [
        "population_2020",
        "median_household_income",
    ]
    for col in numeric_cols:
        if col in model_df.columns:
            model_df[col] = pd.to_numeric(model_df[col], errors="coerce")

    # Keep urban/rural as categorical text if present
    if "urban_rural_status" in model_df.columns:
        model_df["urban_rural_status"] = (
            model_df["urban_rural_status"].astype(str).str.strip()
        )

    # Check parish matches
    missing_covariate_parishes = sorted(
        set(model_df.loc[model_df.isna().any(axis=1), "parish"].dropna().unique())
        - set(covariates["parish"].unique())
    )

    model_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved modeling dataset to:\n{OUTPUT_FILE}")
    print(f"\nRows: {len(model_df):,}")
    print(f"Columns: {len(model_df.columns)}")

    print("\nPreview:")
    print(model_df.head())

    print("\nMissingness summary (top 15):")
    missing_summary = pd.DataFrame(
        {
            "missing_count": model_df.isna().sum(),
            "missing_percent": model_df.isna().mean() * 100,
        }
    ).sort_values("missing_percent", ascending=False)
    print(missing_summary.head(15))

    if missing_covariate_parishes:
        print("\nParishes with possible covariate merge issues:")
        print(missing_covariate_parishes)
    else:
        print("\nNo obvious parish-name merge issues detected.")


if __name__ == "__main__":
    main()