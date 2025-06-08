"""
Unit test on bloomberg data
"""

import pandas as pd
import pytest

try:
    from pull_bloomberg_cip_data import *
    import pull_bloomberg_cip_data as pull_bloomberg_cip_data
except ModuleNotFoundError:
    from src.pull_bloomberg_cip_data import *
    import src.pull_bloomberg_cip_data as pull_bloomberg_cip_data

try:
    from settings import config
except ModuleNotFoundError:
    from src.settings import config



DATA_DIR = config("DATA_DIR")

# double check here
def test_pull_bloomberg_cip_data_load_raw():
    df = pull_bloomberg_cip_data.load_raw(end = '2020-01-01')
    # Test if the function returns a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Test if the DataFrame has the expected columns
    expected_columns = ['AUD_CURNCY', 'CAD_CURNCY', 'CHF_CURNCY', 'EUR_CURNCY', 'GBP_CURNCY',
       'JPY_CURNCY', 'NZD_CURNCY', 'SEK_CURNCY', 'AUD_CURNCY3M',
       'CAD_CURNCY3M', 'CHF_CURNCY3M', 'EUR_CURNCY3M', 'GBP_CURNCY3M',
       'JPY_CURNCY3M', 'NZD_CURNCY3M', 'SEK_CURNCY3M', 'AUD_IR', 'CAD_IR',
       'CHF_IR', 'EUR_IR', 'GBP_IR', 'JPY_IR', 'NZD_IR', 'SEK_IR', 'USD_IR']
    assert all(col in df.columns for col in expected_columns)




def test_pull_bloomberg_cip_data_load_raw():
    df = pull_bloomberg_cip_data.load_raw(end = '2020-01-04')
    
    # Test if the default date range has the expected start date and end date
    assert df.index.min() == pd.Timestamp('2010-01-04')
    assert df.index.max() >= pd.Timestamp('2020-01-01')


def test_pull_bloomberg_cip_data_compute_cip():
    df = pull_bloomberg_cip_data.compute_cip(end='2020-01-01')

    # Ensure the returned object is a DataFrame
    assert isinstance(df, pd.DataFrame)

    # Ensure the DataFrame is not empty
    assert not df.empty, "The DataFrame should not be empty."

    # Find the column with the max value
    max_column = df.max().idxmax()

    # Ensure the max column is 'CIP_CHF_ln'
    assert max_column == 'CIP_CHF_ln', f"Expected 'CIP_CHF_ln', but got {max_column}."

    # Ensure the max value is not NaN
    max_value = df[max_column].max()
    assert not pd.isna(max_value), f"Max value in column {max_column} is NaN."


