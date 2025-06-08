import pandas as pd
import pytest
from settings import config
import pull_ken_french_data
from pathlib import Path

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

# Test to confirm that pull_ken_french_excel creates an Excel file with the expected sheets
def test_pull_ken_french_excel(tmp_path, monkeypatch):
    # Dummy data to simulate the output of web.DataReader
    def dummy_datareader(dataset_name, source, start, end):
        descr = f"Dummy description for {dataset_name}"
        df_dummy = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": [4, 5, 6]
        })
        return {"DESCR": descr, "0": df_dummy}
    
    # Monkeypatch the web module in pull_ken_french_data with a dummy DataReader
    dummy_web = type("DummyWeb", (), {"DataReader": dummy_datareader})
    monkeypatch.setattr(pull_ken_french_data, "web", dummy_web)
    
    # Use tmp_path as the data directory
    excel_path = pull_ken_french_data.pull_ken_french_excel(
        dataset_name="Test_Dataset",
        data_dir=tmp_path,
        log=False,
        start_date="2020-01-01",
        end_date="2020-12-31"
    )
    
    # Check that the Excel file was created
    excel_file = Path(excel_path)
    assert excel_file.exists(), "Excel file was not created by pull_ken_french_excel"
    
    # Verify that the Excel file contains the expected sheets: "Description" and "0"
    xls = pd.ExcelFile(excel_file)
    assert "Description" in xls.sheet_names, "Description sheet missing in Excel file"
    assert "0" in xls.sheet_names, "Data sheet '0' missing in Excel file"

# Test to confirm that load_returns returns the expected DataFrame for value-weighted data
def test_load_returns(tmp_path, monkeypatch):
    dataset_name = "Test_Dataset"
    excel_name = f"{dataset_name.replace('/', '_')}.xlsx"
    excel_file = tmp_path / excel_name
    
    # Create a dummy Excel file with sheet "0"
    df_dummy = pd.DataFrame({
        "col1": [10, 20],
        "col2": [30, 40]
    })
    with pd.ExcelWriter(excel_file) as writer:
        df_dummy.to_excel(writer, sheet_name="0", index=False)
    
    monkeypatch.setattr(pull_ken_french_data, "DATA_DIR", tmp_path)
    
    df_loaded = pull_ken_french_data.load_returns(dataset_name=dataset_name, weighting="value-weighted", data_dir=tmp_path)
    pd.testing.assert_frame_equal(df_loaded, df_dummy)

# Test to confirm that load_sheet returns the correct output for the Description and data sheets
def test_load_sheet(tmp_path, monkeypatch):
    dataset_name = "Test_Dataset"
    excel_name = f"{dataset_name.replace('/', '_')}.xlsx"
    excel_file = tmp_path / excel_name
    
    # Create a dummy Excel file with a "Description" sheet and a data sheet "0"
    df_descr = pd.DataFrame([["This is a dummy description"]])
    df_dummy = pd.DataFrame({
        "col1": [5, 10],
        "col2": [15, 20]
    })
    with pd.ExcelWriter(excel_file) as writer:
        df_descr.to_excel(writer, sheet_name="Description", index=False)
        df_dummy.to_excel(writer, sheet_name="0", index=False)
    
    monkeypatch.setattr(pull_ken_french_data, "DATA_DIR", tmp_path)
    
    descr = pull_ken_french_data.load_sheet(dataset_name=dataset_name, sheet_name="Description", data_dir=tmp_path)
    assert descr == "This is a dummy description", "load_sheet did not return the expected description"
    
    df_loaded = pull_ken_french_data.load_sheet(dataset_name=dataset_name, sheet_name="0", data_dir=tmp_path)
    pd.testing.assert_frame_equal(df_loaded, df_dummy)
