import pandas as pd
from config import PROJECT_ROOT

DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

ANALYSIS_FILE = DATA_PROCESSED / "lcd_analysis_2015_2025_daily_filled.csv"
THRESHOLD_FILE = DATA_PROCESSED / "parish_baseline_apparent_temp_thresholds.csv"

# overwrite the same analysis file
OUTPUT_FILE = ANALYSIS_FILE


def assign_consecutive_heatwave_flags(group):
    """
    For one parish, mark days as heat-wave only if they belong to
    a run of at least 2 consecutive exceedance days.
    """
    group = group.sort_values("date").copy()

    # basic exceedance indicator
    group["exceeds_threshold"] = (
        group["min_apparent_temp_c"] > group["apparent_temp_85th_percentile"]
    )

    # create run IDs whenever date is not consecutive or exceedance changes
    prev_date = group["date"].shift(1)
    prev_exceed = group["exceeds_threshold"].shift(1)

    new_run = (group["exceeds_threshold"] != prev_exceed) | (
        (group["date"] - prev_date).dt.days != 1
    )

    group["run_id"] = new_run.cumsum()

    # size of each run
    group["run_length"] = group.groupby("run_id")["run_id"].transform("size")

    # heat-wave only if in exceedance run with length >= 2
    group["heatwave_flag"] = "non-heat-wave"
    heatwave_mask = group["exceeds_threshold"] & (group["run_length"] >= 2)
    group.loc[heatwave_mask, "heatwave_flag"] = "heat-wave"

    return group


def main():
    analysis_df = pd.read_csv(ANALYSIS_FILE)
    threshold_df = pd.read_csv(THRESHOLD_FILE)

    analysis_df["date"] = pd.to_datetime(analysis_df["date"], errors="coerce")
    analysis_df["min_apparent_temp_c"] = pd.to_numeric(
        analysis_df["min_apparent_temp_c"], errors="coerce"
    )
    threshold_df["apparent_temp_85th_percentile"] = pd.to_numeric(
        threshold_df["apparent_temp_85th_percentile"], errors="coerce"
    )

    # merge parish-specific thresholds into analysis dataset
    merged = analysis_df.merge(threshold_df, on="parish", how="left")

    # flag heat-wave / non-heat-wave parish-days
    flagged = (
        merged.groupby("parish", group_keys=False)
        .apply(assign_consecutive_heatwave_flags)
        .reset_index(drop=True)
    )

    flagged.to_csv(OUTPUT_FILE, index=False)

    print(f"Updated analysis file saved to:\n{OUTPUT_FILE}")
    print("\nPreview:")
    print(
        flagged[
            [
                "parish",
                "date",
                "min_apparent_temp_c",
                "apparent_temp_85th_percentile",
                "exceeds_threshold",
                "run_length",
                "heatwave_flag",
            ]
        ].head(15)
    )

    print("\nHeat-wave flag counts:")
    print(flagged["heatwave_flag"].value_counts())


if __name__ == "__main__":
    main()