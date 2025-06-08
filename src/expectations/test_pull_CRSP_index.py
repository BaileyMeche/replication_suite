import pandas as pd
import pytest
from settings import config
import pull_CRSP_index
from pathlib import Path

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

# Test to confirm that the function returns a pandas DataFrame with the expected columns
def test_pull_crsp_functionality():
    df = pull_CRSP_index.pull_crsp_value_weighted_index(data_dir=Path(DATA_DIR), start_date=START_DATE, end_date=END_DATE, log=False)

    # Test if the function returns a pandas DataFrame
    assert isinstance(df, pd.DataFrame), "Function did not return a DataFrame"

    # Test if the DataFrame has the expected columns
    expected_columns = ['date', 'value_weighted_return']
    assert all(col in df.columns for col in expected_columns), "DataFrame is missing expected columns"

# Test to confirm that the data has valid start and end dates
def test_pull_crsp_data_validity():
    df = pull_CRSP_index.pull_crsp_value_weighted_index(data_dir=Path(DATA_DIR), start_date=START_DATE, end_date=END_DATE, log=False)
    
    # Convert the 'date' column to ensure correct comparison
    df['date'] = pd.to_datetime(df['date'])

    # Test if the default date range has the expected start and end date
    assert df['date'].min() >= pd.Timestamp(START_DATE), "Start date is earlier than expected"
    assert df['date'].max() <= pd.Timestamp(END_DATE), "End date is later than expected"

