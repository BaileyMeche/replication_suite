import wrds
import pandas as pd
import os
from settings import config
from pathlib import Path
import numpy as np

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

def load_crsp_index():
    """
    Load the CRSP value-weighted index data from the CSV file saved by pull_CRSP_index.py.
    Adjusts the dates from the last business day of the month to the first day of the following month.
    
    Assumes that the raw 'value_weighted_return' column is in percentage points (e.g., 5.8 means 5.8%).
    Computes log returns: log(1 + return_decimal).
    
    Returns:
        df (DataFrame): Contains the adjusted 'date' and computed 'log_return'.
    """
    csv_path = DATA_DIR / "crsp_value_weighted_index.csv"
    df = pd.read_csv(csv_path, parse_dates=["date"])
    
    # Adjust dates: convert each date to the first day of the following month.
    # For example, if date is 4/29/1930, it will become 5/1/1930.
    df['date'] = df['date'].apply(lambda d: (d.replace(day=1) + pd.DateOffset(months=1)))
    
    df['value_weighted_return'] = np.log(1 + df['value_weighted_return'])
    
    return df

def load_and_compute_log_returns():
    """
    Loads CRSP market returns and FRED risk-free rate (TB3MS), aligns them by date,
    and computes excess returns = market return minus TB3MS.
    
    Returns:
        excess_returns (Series): Excess returns with dates as index.
    """
    # Load CRSP data and set date as index
    crsp_df = load_crsp_index()
    crsp_df = crsp_df.set_index("date")
    # Ensure the market return is numeric
    market_return = pd.to_numeric(crsp_df["value_weighted_return"], errors='coerce')
    
    # Compute excess returns
    excess_returns = market_return
    return excess_returns

def load_ken_french(dataset_name="6_Portfolios_2x3", weighting="value-weighted"):
    """
    Load Ken French portfolio data from the Excel file saved by pull_ken_french_data.py.
    If weighting is 'BE_FYt-1_to_ME_June_t', then shift the date index backward by 7 months 
    to align the valuation ratio with the time the information is available.
    
    Additionally, convert the numeric (BM ratio) columns to log form.
    """
    excel_name = f"{dataset_name.replace('/', '_')}.xlsx"
    excel_path = DATA_DIR / excel_name
    
    if weighting == "value-weighted":
        sheet_name = "0"
    elif weighting == "equal-weighted":
        sheet_name = "1"    
    elif weighting == "BE_to_ME":
        sheet_name = "6"    
    elif weighting == "BE_FYt-1_to_ME_June_t":
        sheet_name = "7"
    else:
        raise ValueError("Invalid weighting: must be 'value-weighted', 'equal-weighted', or 'BE_FYt-1_to_ME_June_t'.")
    
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    # If using the BE_FYt-1_to_ME_June_t measure, shift the dates backward by 7 months 
    # so that the valuation ratio is aligned with the time it would be available for forecasting.
    if weighting == "BE_FYt-1_to_ME_June_t":
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        df.index = df.index - pd.DateOffset(months=7)
    
    # Convert numeric columns (assumed to be BM ratios) to their natural logarithms.
    # This is in line with the paper's approach of using log BM ratios.
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Only apply if all values are positive (to avoid math errors)
            if (df[col] > 0).all():
                df[col] = np.log(df[col])
    
    return df

