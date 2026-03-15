import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FINAL = PROJECT_ROOT / "data" / "final"
DATA_FINAL.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_FINAL / "parish_covariates.csv"

# Official source URLs
POP_URL = (
    "https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=county:*&in=state:22"
)
INCOME_URL = "https://api.census.gov/data/2020/acs/acs5?get=NAME,B19013_001E&for=county:*&in=state:22"
RUCC_URL = (
    "https://www.ers.usda.gov/media/5767/2023-rural-urban-continuum-codes.xlsx?v=28875"
)


def normalize_parish_name(name: str) -> str:
    name = str(name).strip()
    name = name.replace(" Parish, Louisiana", "")
    name = name.replace(" parish, louisiana", "")
    name = name.replace(", Louisiana", "")
    name = name.replace(", louisiana", "")
    return name.strip()


def find_fips_column(df: pd.DataFrame) -> str:
    candidates = ["FIPS", "fips", "fips_code", "FIPS_code", "CountyFIPS", "county_fips"]
    for col in candidates:
        if col in df.columns:
            return col

    # fallback: first column containing 'fips'
    for col in df.columns:
        if "fips" in col.lower():
            return col

    raise ValueError("Could not find a FIPS column in the RUCC file.")


def find_rucc_column(df: pd.DataFrame) -> str:
    candidates = [
        "RUCC_2023",
        "RUCC",
        "rucc",
        "Rural_urban_continuum_code_2023",
        "RuralUrbanContinuumCode2023",
    ]
    for col in candidates:
        if col in df.columns:
            return col

    for col in df.columns:
        lower = col.lower()
        if "rucc" in lower or ("continuum" in lower and "code" in lower):
            return col

    raise ValueError("Could not find an RUCC code column in the RUCC file.")


def load_population():
    pop = pd.read_json(POP_URL)
    pop.columns = pop.iloc[0]
    pop = pop.iloc[1:].copy()

    pop = pop.rename(
        columns={
            "NAME": "name",
            "P1_001N": "population",
            "state": "state_fips",
            "county": "county_fips",
        }
    )

    pop["population"] = pd.to_numeric(pop["population"], errors="coerce")
    pop["county_fips"] = pop["county_fips"].astype(str).str.zfill(3)
    pop["fips"] = "22" + pop["county_fips"]
    pop["parish"] = pop["name"].apply(normalize_parish_name)

    return pop[["parish", "fips", "population"]]


def load_income():
    inc = pd.read_json(INCOME_URL)
    inc.columns = inc.iloc[0]
    inc = inc.iloc[1:].copy()

    inc = inc.rename(
        columns={
            "NAME": "name",
            "B19013_001E": "median_household_income",
            "state": "state_fips",
            "county": "county_fips",
        }
    )

    inc["median_household_income"] = pd.to_numeric(
        inc["median_household_income"], errors="coerce"
    )
    inc["county_fips"] = inc["county_fips"].astype(str).str.zfill(3)
    inc["fips"] = "22" + inc["county_fips"]
    inc["parish"] = inc["name"].apply(normalize_parish_name)

    return inc[["parish", "fips", "median_household_income"]]


def load_rucc():
    rucc = pd.read_excel(RUCC_URL)

    print("RUCC columns detected:")
    print(rucc.columns.tolist())

    # find FIPS column
    fips_col = None
    for col in rucc.columns:
        lower = str(col).lower()
        if "fips" in lower:
            fips_col = col
            break

    if fips_col is None:
        raise ValueError("Could not find a FIPS column in the RUCC file.")

    # find RUCC code column
    rucc_col = None
    for col in rucc.columns:
        lower = str(col).lower()
        if "rucc" in lower:
            rucc_col = col
            break
        if "continuum" in lower and "code" in lower:
            rucc_col = col
            break

    if rucc_col is None:
        raise ValueError("Could not find an RUCC code column in the RUCC file.")

    rucc[fips_col] = (
        rucc[fips_col].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(5)
    )
    rucc["fips"] = rucc[fips_col]
    rucc["rucc_code"] = pd.to_numeric(rucc[rucc_col], errors="coerce")

    rucc["urban_rural_status"] = rucc["rucc_code"].apply(
        lambda x: (
            "Urban" if pd.notna(x) and x <= 3 else ("Rural" if pd.notna(x) else None)
        )
    )

    return rucc[["fips", "rucc_code", "urban_rural_status"]]


def main():
    pop = load_population()
    inc = load_income()
    rucc = load_rucc()

    cov = pop.merge(inc[["fips", "median_household_income"]], on="fips", how="left")
    cov = cov.merge(rucc, on="fips", how="left")

    # Keep parish from population source
    cov = (
        cov[
            [
                "parish",
                "fips",
                "population",
                "median_household_income",
                "rucc_code",
                "urban_rural_status",
            ]
        ]
        .sort_values("parish")
        .reset_index(drop=True)
    )

    cov.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved parish covariates to:\n{OUTPUT_FILE}")
    print(f"\nRows: {len(cov)}")
    print("\nPreview:")
    print(cov.head(10))

    print("\nMissingness:")
    print(cov.isna().sum())


if __name__ == "__main__":
    main()
