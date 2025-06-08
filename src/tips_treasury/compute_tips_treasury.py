import os
from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import numpy as np
from pathlib import Path
from decouple import config

scheduler = config("DASK_SCHEDULER_ADDRESS", default=None)

if scheduler:
    client = Client(scheduler)
else:
    # fall‐back to launching a local cluster
    client = Client()
print("Connected to scheduler at:", client.scheduler.address)

# Configuration
DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# ------------------------------------------------------------------------------
# Import inflation swap data (from CSV), return Dask DataFrame
# ------------------------------------------------------------------------------
def import_inflation_swap_data():
    swaps_path = Path(OUTPUT_DIR) / "treasury_inflation_swaps.csv"
    pdf = pd.read_csv(swaps_path, parse_dates=["Dates"])
    col_map = {
        "Dates": "date",
        "USSWITA BGN Curncy": "inf_swap_1m",
        "USSWITC BGN Curncy": "inf_swap_3m",
        "USSWITF BGN Curncy": "inf_swap_6m",
        "USSWIT1 BGN Curncy": "inf_swap_1y",
        "USSWIT2 BGN Curncy": "inf_swap_2y",
        "USSWIT3 BGN Curncy": "inf_swap_3y",
        "USSWIT4 BGN Curncy": "inf_swap_4y",
        "USSWIT5 BGN Curncy": "inf_swap_5y",
        "USSWIT10 BGN Curncy": "inf_swap_10y",
        "USSWIT20 BGN Curncy": "inf_swap_20y",
        "USSWIT30 BGN Curncy": "inf_swap_30y"
    }
    pdf = pdf.rename(columns=col_map)
    inf_cols = [c for c in col_map.values() if c.startswith('inf_swap_')]
    for col in inf_cols:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce') / 100.0
    pdf = pdf[['date'] + [c for c in inf_cols if c in pdf.columns]]
    pdf['date'] = pd.to_datetime(pdf['date'], errors='coerce')
    pdf = pdf.dropna(subset=['date'])
    return dd.from_pandas(pdf, npartitions=len(client.ncores()))

# ------------------------------------------------------------------------------
# Import nominal Treasury yields (parquet)
# ------------------------------------------------------------------------------
def import_treasury_yields():
    nom_path = Path(DATA_DIR) / "fed_yield_curve.parquet"
    df = pd.read_parquet(nom_path)

    # if the DataFrame’s index is already named 'date' or 'Date', promote it to a column
    idx_name = df.index.name
    if idx_name in ("date", "Date"):
        df = df.reset_index().rename(columns={idx_name: "date"})

    # otherwise, create a 'date' column from the index
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(df.index, errors="coerce")

    # compute zero-coupon yields
    for t in (2, 5, 10, 20):
        col = f"SVENY{t:02d}"
        if col in df.columns:
            df[f"nom_zc{t}"] = 1e4 * (np.exp(df[col] / 100.0) - 1)

    # keep only date + the newly created nom_zc columns
    cols = ["date"] + [f"nom_zc{t}" for t in (2, 5, 10, 20) if f"nom_zc{t}" in df.columns]
    df = df[cols]

    return dd.from_pandas(df, npartitions=len(client.ncores()))


# ------------------------------------------------------------------------------
# Import TIPS real yields (parquet)
# ------------------------------------------------------------------------------
def import_tips_yields():
    real_path = Path(DATA_DIR) / "fed_tips_yield_curve.parquet"
    pdf = pd.read_parquet(real_path)
    if 'Date' in pdf.columns:
        pdf = pdf.rename(columns={'Date':'date'})
    if 'date' not in pdf.columns:
        pdf['date'] = pd.to_datetime(pdf.index, errors='coerce')
    pdf['date'] = pd.to_datetime(pdf['date'], errors='coerce')
    for t in [2,5,10,20]:
        col = f"TIPSY{str(t).zfill(2)}"
        if col in pdf.columns:
            pdf[f"real_cc{t}"] = pdf[col] / 100.0
    cols = ['date'] + [f"real_cc{t}" for t in [2,5,10,20] if f"real_cc{t}" in pdf.columns]
    pdf = pdf[cols]
    return dd.from_pandas(pdf, npartitions=len(client.ncores()))

# ------------------------------------------------------------------------------
# Merge data and compute arbitrage series
# ------------------------------------------------------------------------------
def compute_tips_treasury():
    swaps = import_inflation_swap_data()
    nom   = import_treasury_yields()
    tips  = import_tips_yields()

    df = dd.merge(tips, nom, on='date', how='inner')
    df = dd.merge(df, swaps, on='date', how='inner')

    # Compute all tenors in one pass
    def part_calc_all(part):
        data = {}
        for t in [2,5,10,20]:
            real_arr = part[f"real_cc{t}"].to_numpy()
            inf_arr  = part.get(f"inf_swap_{t}y", np.zeros_like(real_arr)).to_numpy()
            rf_vals  = 1e4 * (np.exp(real_arr + np.log1p(inf_arr)) - 1)
            data[f"tips_treas_{t}_rf"] = rf_vals
            data[f"mi_{t}"] = np.isnan(rf_vals).astype(int)
            data[f"arb_{t}"] = rf_vals - part[f"nom_zc{t}"].to_numpy()
        return pd.DataFrame(data, index=part.index)

    # Metadata for new columns
    meta = {}
    for t in [2,5,10,20]:
        meta.update({
            f"tips_treas_{t}_rf": 'f8',
            f"mi_{t}": 'i8',
            f"arb_{t}": 'f8'
        })
    comp = df.map_partitions(part_calc_all, meta=meta)
    df = dd.concat([df, comp], axis=1)

    # Filter rows with too many missing
    mi_cols = [f"mi_{t}" for t in [2,5,10,20]]
    df['miss_count'] = df[mi_cols].sum(axis=1)
    df = df[df['miss_count'] < 4]
    df = df.drop(mi_cols + ['miss_count'], axis=1)

    # Reorder
    keep = ['date'] + [c for c in df.columns if c.startswith('real_cc')] + \
           [c for c in df.columns if c.startswith('nom_zc')] + \
           [c for c in df.columns if c.startswith('tips_treas_')] + \
           [c for c in df.columns if c.startswith('arb_')]
    df = df[keep]

    # Compute and save
    result = df.compute()
    out_path = Path(DATA_DIR) / "tips_treasury_implied_rf.parquet"
    result.to_parquet(out_path, compression="snappy")
    print(f"Data saved to {out_path}")
    return result

# Run as script
if __name__ == "__main__":
    compute_tips_treasury()
