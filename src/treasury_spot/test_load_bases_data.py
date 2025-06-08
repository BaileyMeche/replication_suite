import pytest
import pandas as pd
from load_bases_data import load_combined_spreads_wide

# Reference data URL (same as used in function)
REFERENCE_URL = "https://www.dropbox.com/scl/fi/81jm3dbe856i7p17rjy87/arbitrage_spread_wide.dta?rlkey=ke78u464vucmn43zt27nzkxya&st=59g2n7dt&dl=1"

@pytest.fixture
def df_loaded():
    """Load the dataset using the function."""
    return load_combined_spreads_wide()

def test_dataset_load(df_loaded):
    """Ensure the dataset loads successfully."""
    assert not df_loaded.empty, "Dataset failed to load!"
    assert df_loaded.shape[0] > 0, "Dataset has no rows!"
    assert df_loaded.shape[1] > 0, "Dataset has no columns!"

def test_index_is_date(df_loaded):
    """Ensure the index is correctly set to 'date'."""
    assert df_loaded.index.name == "date", "Index is not set to 'date'!"
    assert isinstance(df_loaded.index, pd.DatetimeIndex), "Index is not a DatetimeIndex!"

def test_expected_columns_exist(df_loaded):
    """Ensure some key expected columns exish."""
    expected_columns = {"Treasury_SF_02Y", "Treasury_SF_05Y", "Treasury_SF_10Y"}
    assert expected_columns.issubset(df_loaded.columns), f"Missing columns: {expected_columns - set(df_loaded.columns)}"

if __name__ == "__main__":
    pytest.main()
