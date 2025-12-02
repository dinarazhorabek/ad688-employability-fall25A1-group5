import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Last 5 years
ACS_YEARS = [2024, 2023, 2022, 2021, 2020]

def get_acs_computer_math_gender_by_state(year: int) -> pd.DataFrame:
    """
    Fetch ACS S2401 data for:
      - Computer and mathematical occupations
      - Male & female counts
      - All U.S. states (incl. DC)
      - For a given ACS 1-year estimate year
    """
    base_url = f"https://api.census.gov/data/{year}/acs/acs1/subject"

    vars_to_get = [
        "NAME",
        "S2401_C01_007E",  # total workers (all genders)
        "S2401_C02_007E",  # male workers
        "S2401_C04_007E",  # female workers
    ]

    params = {
        "get": ",".join(vars_to_get),
        "for": "state:*",
    }

    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    data = resp.json()

    header = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=header)

    df = df.rename(
        columns={
            "NAME": "state_name",
            "state": "state_fips",
            "S2401_C01_007E": "cm_total",
            "S2401_C02_007E": "cm_male",
            "S2401_C04_007E": "cm_female",
        }
    )

    # Convert numeric values
    for col in ["cm_total", "cm_male", "cm_female"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add year column
    df["year"] = year

    # Drop any rows without total
    df = df[df["cm_total"] > 0].reset_index(drop=True)
    return df


if __name__ == "__main__":
    all_years = []

    for yr in ACS_YEARS:
        try:
            print(f"Fetching ACS S2401 for year {yr}...")
            df_year = get_acs_computer_math_gender_by_state(yr)
            all_years.append(df_year)
        except Exception as e:
            print(f"Failed for year {yr}: {e}")

    # Combine all 5 years into one file
    if all_years:
        df_all = pd.concat(all_years, ignore_index=True)

        out_path = "empl-web/data/acs_computer_math_gender_by_state_5yrs.csv"
        df_all.to_csv(out_path, index=False)
        print(f"\nSaved combined dataset: {out_path}")
        # print(df_all.head())
    else:
        print("No data fetched — check connection or API availability.")