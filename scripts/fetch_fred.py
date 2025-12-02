import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")
print("FRED_API_KEY loaded:", FRED_API_KEY is not None)


def get_fred_series(series_id, start="2000-01-01"):
    """Generic helper to pull one FRED series."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start,
    }

    r = requests.get(url, params=params)
    r.raise_for_status()

    data = r.json()["observations"]
    df = pd.DataFrame(data)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "value"]]


if __name__ == "__main__":
    fred_src = {
        "unemp_women": "LNS14000002",
        "unemp_married_women": "LNS13000315",
        "unemp_men": "LNU03000001",
        "unemp_married_men": "LNS13000150",
        "emp_women": "LNS12000002",
        "emp_married_women": "LNS12000315",
        "emp_men": "LNS12000001",
        "emp_married_men": "LNS12000150",
        "emp_pop_ratio_women": "LNS12300002",
        "emp_pop_ratio_men": "LNS12300001",
        "infra_ann_labor_women": "LREM25FEUSQ156S",
        "infra_ann_labor_men": "LREM25MAUSA156S",
    }
    os.makedirs("empl-web/data", exist_ok=True)

    for src_name, src_code in fred_src.items():
        df = get_fred_series(src_code)
        df.to_csv(f"empl-web/data/fred_{src_name}.csv", index=False)
        print(f"Saved empl-web/data/fred_{src_name}.csv")

    # ---------- 2) STATE-LEVEL UNEMPLOYMENT (all states in ONE file) ----------
    # FRED uses pattern like ALUR, CAUR, TXUR for seasonally adjusted state unemployment.
    state_abbrevs = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
        "DC",
    ]

    all_states = []

    for st in state_abbrevs:
        series_id = f"{st}UR"   # e.g. ALUR, CAUR, TXUR
        try:
            df_state = get_fred_series(series_id)
            df_state["state"] = st
            all_states.append(df_state)
            print(f"Fetched state unemployment for {st} ({series_id})")
        except requests.HTTPError as e:
            print(f"Failed for {st} ({series_id}): {e}")

    if all_states:
        state_unemp = pd.concat(all_states, ignore_index=True)
        state_unemp = state_unemp.sort_values(["date", "state"])
        state_unemp.to_csv("empl-web/data/fred_state_unemp.csv", index=False)
        print("Saved empl-web/data/fred_state_unemp.csv")
    else:
        print("No state data fetched – check FRED_API_KEY or series IDs.")