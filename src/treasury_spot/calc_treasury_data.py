#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
calc_treasury_data.py

This module reads the intermediate pulled data, processes the Treasury Spot-Futures data by:
    - Reshaping from wide to long format.
    - Computing time-to-maturity (TTM) from contract strings.
    - Interpolating OIS rates.
    - Calculating arbitrage spreads with outlier cleaning.
    - Plotting arbitrage spreads for selected tenors.
    - Saving the final output in wide format.
    
Usage:
    python calc_treasury_data.py
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sys
import os

# Ensure 'src' is in sys.path
sys.path.append(os.path.abspath("./src"))  # Add 'src' to the path

# Now import config from settings.py
from settings import config  # <- FIXED

# Set data directory
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")






# -------------------------
# Helper functions
# -------------------------
def parse_contract_date(contract_str):
    """
    Parse contract string to extract month and year.
    E.g., "DEC 21" -> (12, 2021)
    """
    if pd.isna(contract_str) or not isinstance(contract_str, str):
        return None, None
    month_abbr = contract_str[:3].upper()
    year_str = contract_str[4:6]
    month_map = {'DEC': 12, 'MAR': 3, 'JUN': 6, 'SEP': 9}
    month = month_map.get(month_abbr, np.nan)
    try:
        year = int(year_str) + 2000
    except:
        year = np.nan
    return month, year

def interpolate_ois(ttm, ois_1w, ois_1m, ois_3m, ois_6m, ois_1y):
    """Interpolate the OIS rate based on TTM (in days)."""
    if ttm <= 7:
        return ois_1w
    elif 7 < ttm <= 30:
        return ((30 - ttm) / 23) * ois_1w + ((ttm - 7) / 23) * ois_1m
    elif 30 < ttm <= 90:
        return ((90 - ttm) / 60) * ois_1m + ((ttm - 30) / 60) * ois_3m
    elif 90 < ttm <= 180:
        return ((180 - ttm) / 90) * ois_3m + ((ttm - 90) / 90) * ois_6m
    else:
        return ((360 - ttm) / 180) * ois_6m + ((ttm - 180) / 180) * ois_1y

def rolling_outlier_flag(df, group_col, date_col, value_col, window_days=45, threshold=10):
    """
    Flag outliers using a rolling window (±45 days) per group.
    """
    df = df.copy()
    df['bad_price'] = False
    df[date_col] = pd.to_datetime(df[date_col])
    df.sort_values(date_col, inplace=True)
    
    for name, group in df.groupby(group_col):
        for idx, row in group.iterrows():
            curr_date = row[date_col]
            window_mask = (group[date_col] >= curr_date - timedelta(days=window_days)) & \
                          (group[date_col] <= curr_date + timedelta(days=window_days)) & \
                          (group.index != idx)
            window_vals = group.loc[window_mask, value_col]
            if len(window_vals) > 0:
                median_val = window_vals.median()
                abs_dev = abs(row[value_col] - median_val)
                mad = window_vals.subtract(median_val).abs().mean()
                if mad > 0 and (abs_dev / mad) >= threshold:
                    df.at[idx, 'bad_price'] = True
    return df

def calc_treasury():
    # Load intermediate data
    data_dir = DATA_DIR

    # Load the required CSV files from _data
    treasury_file = os.path.join(data_dir, "treasury_df.csv")
    ois_file = os.path.join(data_dir, "ois_df.csv")
    last_day_file = os.path.join(data_dir, "last_day_df.csv")

    # Read the datasets
    df = pd.read_csv(treasury_file)
    df_ois = pd.read_csv(ois_file)
    last_day_df = pd.read_csv(last_day_file)

    # Convert date columns to datetime format
    df["Date"] = pd.to_datetime(df["Date"])
    df_ois["Date"] = pd.to_datetime(df_ois["Date"])
    last_day_df["Date"] = pd.to_datetime(last_day_df["Date"])

    # -------------------------
    # Reshape from wide to long format
    stubnames = ["Contract_1", "Contract_2", "Implied_Repo_1", "Implied_Repo_2",
                 "Vol_1", "Vol_2", "Price_1", "Price_2"]
    df_long = pd.wide_to_long(df, stubnames=stubnames, i="Date", j="Tenor", sep="_", suffix=r'\d+').reset_index()

    # Filter dates > June 22, 2004
    cutoff_date = datetime(2004, 6, 22)
    df_long = df_long[df_long["Date"] > cutoff_date].copy()
    
    # -------------------------
    # Compute time-to-maturity for contracts v=1 and v=2
    for v in [1, 2]:
        contract_col = f"Contract_{v}"
        ttm_col = f"TTM_{v}"
        mat_date_col = f"Mat_Date_{v}"
        
        # Parse contract string to get month and year
        df_long[[f"Mat_Month_{v}", f"Mat_Year_{v}"]] = df_long[contract_col].apply(
            lambda s: pd.Series(parse_contract_date(s))
        )
        
        # Merge with last_day_df to get the day-of-month
        df_long = df_long.merge(last_day_df, left_on=[f"Mat_Month_{v}", f"Mat_Year_{v}"], 
                                right_on=["Mat_Month", "Mat_Year"], how="left", suffixes=("", f"_{v}"))
        # For specific contracts without a business day, set Mat_Day = 31
        cond_special = df_long[contract_col].isin(["DEC 21", "MAR 22"])
        df_long.loc[cond_special, "Mat_Day"] = 31
        
        def make_mat_date(row):
            try:
                return datetime(int(row[f"Mat_Year_{v}"]), int(row[f"Mat_Month_{v}"]), int(row["Mat_Day"]))
            except Exception:
                return pd.NaT
        df_long[mat_date_col] = df_long.apply(make_mat_date, axis=1)
        df_long[ttm_col] = (df_long[mat_date_col] - df_long["Date"]).dt.days
        
        # Clean up temporary columns
        df_long.drop(columns=[f"Mat_Month_{v}", f"Mat_Year_{v}", "Mat_Month", "Mat_Year", "Mat_Day"], 
                      inplace=True, errors='ignore')
    
    # -------------------------
    
    # Merge with USD OIS Rates on Date
    df_ois['Date'] = pd.to_datetime(df_ois['Date'])
    df_long = df_long.merge(df_ois, left_on="Date", right_on="Date", how="left")
    # df_long.drop(columns=["Date"], inplace=True)
    


    # -------------------------
    # Interpolate OIS rates for contracts v=1 and v=2
    for v in [1, 2]:
        ttm_col = f"TTM_{v}"
        ois_col = f"OIS_{v}"
        df_long[ois_col] = df_long.apply(lambda row: 
                                         interpolate_ois(row[ttm_col],
                                                         row.get("OIS_1W", np.nan),
                                                         row.get("OIS_1M", np.nan),
                                                         row.get("OIS_3M", np.nan),
                                                         row.get("OIS_6M", np.nan),
                                                         row.get("OIS_1Y", np.nan)
                                                        ) if pd.notnull(row[ttm_col]) else np.nan, axis=1)
    
    # -------------------------
    # Compute Treasury arbitrage spreads
    df_long["Arb_N"] = (df_long["Implied_Repo_1"] - df_long["OIS_1"]) * 100
    df_long["Arb_D"] = (df_long["Implied_Repo_2"] - df_long["OIS_2"]) * 100
    df_long["arb"] = df_long["Arb_D"]   # Use deferred contract
    
    # Outlier cleanup: flag observations based on a 45-day rolling window.
    df_long = rolling_outlier_flag(df_long, group_col="Tenor", date_col="Date", value_col="arb",
                                   window_days=45, threshold=10)
    df_long.loc[df_long["bad_price"] & df_long["arb"].notnull(), "arb"] = np.nan
    
    # Drop rows without trading volume in deferred contract (Vol_2)
    df_long = df_long[df_long["Vol_2"].notnull()].copy()
    
    # -------------------------
    # Plot arbitrage spread for selected tenors.
    output_dir = OUTPUT_DIR 
    os.makedirs(output_dir, exist_ok=True)
    for tenor in [2, 5, 10, 20, 30]:
        df_plot = df_long[df_long["Tenor"] == str(tenor)]
        if df_plot.empty:
            continue
        plt.figure(figsize=(10, 5))
        plt.plot(df_plot["Date"], df_plot["arb"], label=f"Tenor = {tenor} years")
        plt.ylabel("Arbitrage Spread (bps)")
        plt.xlabel("")
        plt.title(f"Tenor = {tenor} years")
        plt.legend()
        plt.tight_layout()
        plot_path = os.path.join(output_dir, f"arbitrage_spread_{tenor}.pdf")
        plt.savefig(plot_path)
        plt.close()
        print(f"Saved plot: {plot_path}")
    
    # -------------------------
    # Prepare final output
    df_long["T_SF_Rf"] = df_long["Implied_Repo_2"] * 100
    df_long.loc[df_long["bad_price"] & df_long["T_SF_Rf"].notnull(), "T_SF_Rf"] = np.nan
    df_long["rf_ois_t_sf_mat"] = df_long["OIS_2"] * 100
    df_long["T_SF_TTM"] = df_long["TTM_2"]
    df_out = df_long[["Date", "Tenor", "T_SF_Rf", "rf_ois_t_sf_mat", "T_SF_TTM"]].copy()
    
    # Reshape output to wide format (one row per date)
    df_wide = df_out.pivot(index="Date", columns="Tenor")
    df_wide.columns = ['_'.join([str(c) for c in col]).strip() for col in df_wide.columns.values]
    df_wide.reset_index(inplace=True)
    
    # Rename columns to match output convention
    rename_dict = {
        "T_SF_Rf_2": "tfut_2_rf",
        "T_SF_Rf_5": "tfut_5_rf",
        "T_SF_Rf_10": "tfut_10_rf",
        "T_SF_Rf_20": "tfut_20_rf",
        "T_SF_Rf_30": "tfut_30_rf",
        "T_SF_TTM_2": "tfut_2_ttm",
        "T_SF_TTM_5": "tfut_5_ttm",
        "T_SF_TTM_10": "tfut_10_ttm",
        "T_SF_TTM_20": "tfut_20_ttm",
        "T_SF_TTM_30": "tfut_30_ttm",
        "rf_ois_t_sf_mat_2": "tfut_2_ois",
        "rf_ois_t_sf_mat_5": "tfut_5_ois",
        "rf_ois_t_sf_mat_10": "tfut_10_ois",
        "rf_ois_t_sf_mat_20": "tfut_20_ois",
        "rf_ois_t_sf_mat_30": "tfut_30_ois"
    }
    df_wide.rename(columns=rename_dict, inplace=True)

    df_wide["Treasury_SF_2Y"] = df_wide["tfut_2_rf"] - df_wide["tfut_2_ois"]
    df_wide["Treasury_SF_5Y"] = df_wide["tfut_5_rf"] - df_wide["tfut_5_ois"]
    df_wide["Treasury_SF_10Y"] = df_wide["tfut_10_rf"] - df_wide["tfut_10_ois"]
    df_wide["Treasury_SF_20Y"] = df_wide["tfut_20_rf"] - df_wide["tfut_20_ois"]
    df_wide["Treasury_SF_30Y"] = df_wide["tfut_30_rf"] - df_wide["tfut_30_ois"]
    # Select relevant columns
    df_out = df_wide[["Date", "Treasury_SF_2Y", "Treasury_SF_5Y", "Treasury_SF_10Y", "Treasury_SF_20Y", "Treasury_SF_30Y"]].copy()
    df_out.fillna(method='ffill', inplace=True)

    
    # Save final output as Stata .dta file (or CSV if preferred)
    output_file = os.path.join(DATA_DIR, "treasury_sf_output.csv")
    df_out.to_csv(output_file, index=False)




    print(f"Final output saved to {output_file}")
    
if __name__ == '__main__':
    calc_treasury()
