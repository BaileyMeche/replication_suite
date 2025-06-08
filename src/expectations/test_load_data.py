import pandas as pd
import numpy as np
import pytest
from pathlib import Path
from settings import config
import load_data

# Test for load_crsp_index
def test_load_crsp_index(tmp_path, monkeypatch):
    # Create a dummy CSV file with known data
    data = {
        "date": ["2020-01-31"],
        "value_weighted_return": [0.05]
    }
    df_dummy = pd.DataFrame(data)
    csv_file = tmp_path / "crsp_value_weighted_index.csv"
    df_dummy.to_csv(csv_file, index=False)
    
    # Override DATA_DIR to use tmp_path
    monkeypatch.setattr(load_data, "DATA_DIR", tmp_path)
    
    # Call load_crsp_index
    df = load_data.load_crsp_index()
    
    # Check that the date is adjusted correctly: "2020-01-31" -> "2020-02-01"
    expected_date = pd.Timestamp("2020-02-01")
    assert df["date"].iloc[0] == expected_date, "Date not adjusted correctly"
    
    # Check that value_weighted_return is computed as np.log(1 + 0.05)
    expected_return = np.log(1 + 0.05)
    np.testing.assert_almost_equal(df["value_weighted_return"].iloc[0], expected_return)

# Test for load_and_compute_log_returns
def test_load_and_compute_log_returns(tmp_path, monkeypatch):
    # Create a dummy CSV file with known data
    data = {
        "date": ["2020-01-31"],
        "value_weighted_return": [0.05]
    }
    df_dummy = pd.DataFrame(data)
    csv_file = tmp_path / "crsp_value_weighted_index.csv"
    df_dummy.to_csv(csv_file, index=False)
    
    monkeypatch.setattr(load_data, "DATA_DIR", tmp_path)
    
    # Call load_and_compute_log_returns
    excess_returns = load_data.load_and_compute_log_returns()
    
    # Check that the index is the adjusted date "2020-02-01"
    expected_date = pd.Timestamp("2020-02-01")
    assert expected_date in excess_returns.index, "Adjusted date missing in excess returns index"
    
    # Check that the computed log return is correct
    expected_return = np.log(1 + 0.05)
    np.testing.assert_almost_equal(excess_returns.loc[expected_date], expected_return)

# Test for load_ken_french with 'value-weighted' weighting
def test_load_ken_french_value_weighted(tmp_path, monkeypatch):
    dataset_name = "6_Portfolios_2x3"
    excel_name = f"{dataset_name.replace('/', '_')}.xlsx"
    excel_file = tmp_path / excel_name
    
    # Create a dummy DataFrame with a Date column and numeric columns
    dates = pd.date_range(start="2020-01-31", periods=2, freq="M")
    df_dummy = pd.DataFrame({
        "Date": dates,
        "A": [10, 20],
        "B": [100, 200]
    })
    
    # Write to Excel with sheet name "0" for value-weighted weighting
    with pd.ExcelWriter(excel_file) as writer:
        df_dummy.to_excel(writer, sheet_name="0", index=False)
    
    monkeypatch.setattr(load_data, "DATA_DIR", tmp_path)
    
    df_out = load_data.load_ken_french(dataset_name=dataset_name, weighting="value-weighted")
    
    # Check that numeric columns are log-transformed
    expected_A = np.log(10)
    expected_B = np.log(100)
    np.testing.assert_almost_equal(df_out["A"].iloc[0], expected_A)
    np.testing.assert_almost_equal(df_out["B"].iloc[0], expected_B)
    
    # Check that the Date column remains unchanged (not set as index)
    assert "Date" in df_out.columns, "Date column should remain as a column for value-weighted weighting"

# Test for load_ken_french with 'BE_FYt-1_to_ME_June_t' weighting
def test_load_ken_french_BE_FYt_1_to_ME_June_t(tmp_path, monkeypatch):
    dataset_name = "6_Portfolios_2x3"
    excel_name = f"{dataset_name.replace('/', '_')}.xlsx"
    excel_file = tmp_path / excel_name
    
    # Create a dummy DataFrame with a Date column and numeric columns
    dates = pd.date_range(start="2020-06-30", periods=2, freq="M")
    df_dummy = pd.DataFrame({
        "Date": dates,
        "A": [50, 100],
        "B": [500, 1000]
    })
    
    # Write to Excel with sheet name "7" for BE_FYt-1_to_ME_June_t weighting
    with pd.ExcelWriter(excel_file) as writer:
        df_dummy.to_excel(writer, sheet_name="7", index=False)
    
    monkeypatch.setattr(load_data, "DATA_DIR", tmp_path)
    
    df_out = load_data.load_ken_french(dataset_name=dataset_name, weighting="BE_FYt-1_to_ME_June_t")
    
    # For BE_FYt-1_to_ME_June_t, the Date column should be set as the index and shifted back by 7 months.
    original_date = pd.Timestamp("2020-06-30")
    expected_index = original_date - pd.DateOffset(months=7)
    assert df_out.index[0] == expected_index, "Date index not shifted correctly for BE_FYt-1_to_ME_June_t weighting"
    
    # Check that numeric columns are log-transformed
    expected_A = np.log(50)
    expected_B = np.log(500)
    np.testing.assert_almost_equal(df_out["A"].iloc[0], expected_A)
    np.testing.assert_almost_equal(df_out["B"].iloc[0], expected_B)
