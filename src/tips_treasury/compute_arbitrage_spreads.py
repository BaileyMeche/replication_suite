from dask.distributed import Client
import dask.dataframe as dd
import math
from numba import njit
import numpy as np
import pandas as pd
import os
from matplotlib import pyplot as plt
from settings import config
from pathlib import Path

# Load environment variables
DATA_DIR = Path(config("DATA_DIR"))
DATA_MANUAL = Path(config("LOCAL_MANUAL_DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

scheduler = config("DASK_SCHEDULER_ADDRESS", default=None)

if scheduler:
    client = Client(scheduler)
else:
    # fall‐back to launching a local cluster
    client = Client()
print("Connected to scheduler at:", client.scheduler.address)

# Load inflation swaps (Excel or CSV)
def load_inflation_swaps(data_dir=DATA_DIR):
    base = Path(data_dir)
    xlsx = base / "treasury_inflation_swaps.xlsx"
    csv = base / "treasury_inflation_swaps.csv"
    if xlsx.exists():
        df = pd.read_excel(xlsx, sheet_name="Data", skiprows=5, engine="openpyxl")
    elif csv.exists():
        df = pd.read_csv(csv, parse_dates=[0])
    else:
        raise FileNotFoundError(f"No swap file in {data_dir}")
    rename = {'Dates':'date','H':'inf_swap_2y','K':'inf_swap_5y','L':'inf_swap_10y','M':'inf_swap_20y','N':'inf_swap_30y'}
    df = df.rename(columns=rename)
    for col in [c for c in rename.values() if c!='date' and c in df.columns]:
        df[col] = pd.to_numeric(df[col], errors='coerce')/100.0
    df = df[['date'] + [c for c in rename.values() if c in df.columns and c!='date']]
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    return dd.from_pandas(df, npartitions=len(client.ncores()))

# Load nominal yields from parquet
def load_nominal_dd(data_dir=DATA_DIR):
    pdf = pd.read_parquet(Path(data_dir)/"fed_yield_curve.parquet")
    if 'date' not in pdf.columns:
        pdf = pdf.reset_index().rename(columns={pdf.index.name or 'index':'date'})
    pdf['date'] = pd.to_datetime(pdf['date'], errors='coerce')
    return dd.from_pandas(pdf, npartitions=len(client.ncores()))

# Load TIPS yields from parquet
def load_tips_dd(data_dir=DATA_DIR):
    pdf = pd.read_parquet(Path(data_dir)/"fed_tips_yield_curve.parquet")
    if 'date' not in pdf.columns:
        pdf = pdf.reset_index().rename(columns={pdf.index.name or 'index':'date'})
    pdf['date'] = pd.to_datetime(pdf['date'], errors='coerce')
    tip_map = {'TIPSY02':'TIPS_Treasury_02Y','TIPSY05':'TIPS_Treasury_05Y','TIPSY10':'TIPS_Treasury_10Y','TIPSY20':'TIPS_Treasury_20Y'}
    pdf = pdf.rename(columns={k:v for k,v in tip_map.items() if k in pdf.columns})
    return dd.from_pandas(pdf, npartitions=len(client.ncores()))

# Compute zero-coupon yields (bps)
def process_nominal_yields(nom_ddf):
    for t in [2,5,10,20]:
        col = f"SVENY{str(t).zfill(2)}"
        nom_ddf[f"nom_zc{t}"] = nom_ddf[col].map_partitions(
            lambda df: 1e4*(np.exp(df.values/100.0)-1),
            meta=(f"nom_zc{t}", 'f8')
        )
    cols = ['date'] + [f"nom_zc{t}" for t in [2,5,10,20]]
    return nom_ddf[cols]

# Compute real rates from TIPS yields
def process_tips_yields(tips_ddf):
    for t in [2,5,10,20]:
        in_col = f"TIPS_Treasury_{str(t).zfill(2)}Y"
        out_col = f"real_cc{t}"
        if in_col not in tips_ddf.columns:
            raise KeyError(f"Missing {in_col}")
        tips_ddf[out_col] = tips_ddf[in_col].astype(float)/100.0
    cols = ['date'] + [f"real_cc{t}" for t in [2,5,10,20]]
    return tips_ddf[cols]

# Merge and compute arbitrage spreads
# Note: use explicit array extraction to avoid .values misuse
def merge_and_compute_arbitrage(nom_ddf, tips_ddf, swaps_ddf):
    df = dd.merge(tips_ddf, nom_ddf, on='date', how='inner')
    df = dd.merge(df, swaps_ddf, on='date', how='inner')
    for t in [2,5,10,20]:
        swap_col = f"inf_swap_{t}y" if t!=5 else 'inf_swap_5y'
        real = f"real_cc{t}"; nom = f"nom_zc{t}"; rf = f"tips_treas_{t}_rf"; arb = f"arb{t}"
        def compute_rf(part):
            real_arr = part[real].to_numpy()
            swap_arr = part[swap_col].to_numpy() if swap_col in part.columns else np.zeros_like(real_arr)
            return 1e4*(np.exp(real_arr + np.log1p(swap_arr)) - 1)
        df[rf] = df.map_partitions(compute_rf, meta=(rf,'f8'))
        df[arb] = df[rf] - df[nom]
    cols = ['date'] + [f"real_cc{t}" for t in [2,5,10,20]] + \
           [f"nom_zc{t}" for t in [2,5,10,20]] + \
           [f"tips_treas_{t}_rf" for t in [2,5,10,20]] + \
           [f"arb{t}" for t in [2,5,10,20]]
    return df[cols]

if __name__ == "__main__":
    swaps_ddf = load_inflation_swaps(DATA_MANUAL)
    nom_ddf = process_nominal_yields(load_nominal_dd(DATA_DIR))
    tips_ddf = process_tips_yields(load_tips_dd(DATA_DIR))
    merged_ddf = merge_and_compute_arbitrage(nom_ddf, tips_ddf, swaps_ddf)
    result = merged_ddf.compute()
    out = Path(DATA_DIR)/'output'/'tips_treasury_implied_rf.dta'
    os.makedirs(out.parent, exist_ok=True)
    result.to_stata(out, write_index=False)
    print(f"Saved to {out}")
