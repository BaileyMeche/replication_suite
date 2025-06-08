import pandas as pd
import numpy as np
import pytest
import statsmodels.api as sm
from pathlib import Path
from settings import config
import regressions

# --- Dummy Functions for Monkeypatching ---
def dummy_load_and_compute_log_returns():
    # Create synthetic monthly excess returns as a Series with a datetime index.
    dates = pd.date_range(start="2020-01-31", periods=5, freq="M")
    return pd.Series([0.01, 0.02, 0.03, 0.04, 0.05], index=dates)

def dummy_load_ken_french(dataset_name, weighting):
    # Create synthetic Ken French portfolio data with a "Date" column.
    dates = pd.date_range(start="2020-01-31", periods=5, freq="M")
    data = {
        "Date": dates,
        "A": [1, 2, 3, 4, 5],
        "B": [2, 3, 4, 5, 6]
    }
    return pd.DataFrame(data)

# --- Tests for first_stage_regressions ---
def test_first_stage_regressions():
    # Create synthetic y_series and v_df where v = 2 * y_future.
    dates = pd.date_range(start="2020-01-01", periods=5, freq="D")
    y_series = pd.Series([1, 2, 3, 4, 5], index=dates)
    # v_df column "A": v = 2 * (y_series shifted by -1)
    v_values = 2 * y_series.shift(-1)
    v_df = pd.DataFrame({"A": v_values})
    
    phi_dict = regressions.first_stage_regressions(v_df, y_series, h=1)
    # For column "A", the regression slope should be approximately 2.
    assert "A" in phi_dict, "Expected key 'A' not found in phi_dict"
    np.testing.assert_almost_equal(phi_dict["A"], 2, decimal=5)

def test_second_stage_regressions():
    # Create a simple DataFrame and corresponding phi_dict with three assets.
    # For each row, we set the observed values so that Y_i = beta1 * phi_i,
    # with beta1 = 5 for the first row and beta1 = 7 for the second row.
    dates = pd.date_range(start="2020-01-01", periods=2, freq="D")
    v_df = pd.DataFrame({
        "A": [5, 7],
        "B": [10, 14],
        "C": [15, 21]
    }, index=dates)
    phi_dict = {"A": 1, "B": 2, "C": 3}  # Fixed loadings.

    F_series = regressions.second_stage_regressions(v_df, phi_dict)
    # Expected latent factor (slope) for first row is 5, for second row is 7.
    expected = [5.0, 7.0]
    pd.testing.assert_series_equal(
        F_series, 
        pd.Series(expected, index=dates), 
        check_names=False, 
        check_freq=False  # Ignore differences in the frequency attribute.
    )

# --- Tests for third_stage_regression ---
def test_third_stage_regression():
    # Create synthetic F_series and y_series so that y_future = 3 * F.
    dates = pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"])
    F_series = pd.Series([1, 2, 3], index=dates)
    # y_series with one extra element; shifting by -1 will align F_series[0] with y_series[1] and so on.
    y_series = pd.Series([np.nan, 3, 6, 9], index=pd.to_datetime(["2019-12-01", "2020-01-01", "2020-02-01", "2020-03-01"]))
    
    model = regressions.third_stage_regression(F_series, y_series, h=1)
    # The slope on F should be approximately 3.
    np.testing.assert_almost_equal(model.params["F"], 3, decimal=5)

# --- Test for run_in_sample_pls ---
def test_run_in_sample_pls(monkeypatch):
    monkeypatch.setattr(regressions, "load_and_compute_log_returns", dummy_load_and_compute_log_returns)
    monkeypatch.setattr(regressions, "load_ken_french", dummy_load_ken_french)
    dataset_name = "dummy_dataset"
    
    results = regressions.run_in_sample_pls(dataset_name, weighting="dummy", h=1, end_date="2020-12-31")
    expected_keys = {"phi", "F_series", "third_model", "v_df", "y_excess"}
    assert expected_keys.issubset(results.keys()), "run_in_sample_pls did not return expected keys"
    assert not results["F_series"].empty, "F_series is empty"

# --- Test for run_recursive_forecast ---
def test_run_recursive_forecast(monkeypatch):
    monkeypatch.setattr(regressions, "load_and_compute_log_returns", dummy_load_and_compute_log_returns)
    monkeypatch.setattr(regressions, "load_ken_french", dummy_load_ken_french)
    dataset_name = "dummy_dataset"
    
    results = regressions.run_recursive_forecast(
        dataset_name, weighting="dummy", h=1,
        start_train_date="2020-01-31",
        end_train_date="2020-02-29",
        end_forecast_date="2020-05-31"
    )
    expected_keys = {"forecast_series", "actual_series", "R2_oos"}
    assert expected_keys.issubset(results.keys()), "run_recursive_forecast did not return expected keys"
    assert isinstance(results["forecast_series"], pd.Series), "forecast_series is not a Series"
    assert isinstance(results["actual_series"], pd.Series), "actual_series is not a Series"
    assert isinstance(results["R2_oos"], float), "R2_oos is not a float"

# --- Test for select_predictors_annual ---
def test_select_predictors_annual():
    # Create a DataFrame with multiple observations per year.
    dates = pd.to_datetime(["2020-01-15", "2020-06-15", "2021-03-10", "2021-07-20"])
    data = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8]}
    X = pd.DataFrame(data, index=dates)
    
    selected = regressions.select_predictors_annual(X, target_month=6)
    expected_index = pd.to_datetime(["2020-01-01", "2021-01-01"])
    assert all(selected.index == expected_index), "select_predictors_annual did not set index to January 1 of each year"
    pd.testing.assert_series_equal(
        selected.loc[pd.Timestamp("2020-01-01")],
        pd.Series({"A": 2, "B": 6}),
        check_names=False
    )

# --- Test for aggregate_to_annual_returns ---
def test_aggregate_to_annual_returns():
    # Create a Series of 15 monthly returns.
    dates = pd.date_range(start="2020-01-31", periods=15, freq="M")
    returns = pd.Series(np.ones(15) * 0.01, index=dates)
    annual_returns = regressions.aggregate_to_annual_returns(returns)
    # Expect the rolling 12-month sum (shifted by -12) to yield 15 - 12 = 3 observations.
    assert len(annual_returns) == 3, "aggregate_to_annual_returns did not produce expected length"
    np.testing.assert_allclose(annual_returns.values, np.full(3, 0.12), rtol=1e-5)

def test_run_in_sample_pls_annual(monkeypatch):
    def dummy_load_and_compute_log_returns_annual():
        # Produce 48 months of data, ensuring at least 4 distinct annual observations.
        dates = pd.date_range(start="2000-01-31", periods=48, freq="M")
        return pd.Series(np.linspace(0.01, 0.05, 48), index=dates)
    
    def dummy_load_ken_french_annual(dataset_name, weighting):
        # Produce 48 months of dummy portfolio data.
        dates = pd.date_range(start="2000-01-31", periods=48, freq="M")
        data = {
            "Date": dates,
            "A": np.linspace(1, 2, 48),
            "B": np.linspace(2, 3, 48)
        }
        return pd.DataFrame(data)
    
    monkeypatch.setattr(regressions, "load_and_compute_log_returns", dummy_load_and_compute_log_returns_annual)
    monkeypatch.setattr(regressions, "load_ken_french", dummy_load_ken_french_annual)
    dataset_name = "dummy_annual"
    
    results = regressions.run_in_sample_pls_annual(dataset_name, weighting="dummy", h=1, end_date="2003-12-31")
    expected_keys = {"phi", "F_series", "third_model", "X", "y_excess"}
    assert expected_keys.issubset(results.keys()), "run_in_sample_pls_annual did not return expected keys"
    assert not results["F_series"].empty, "Annual F_series is empty"

# --- Test for run_recursive_forecast_annual ---
def test_run_recursive_forecast_annual(monkeypatch):
    def dummy_load_and_compute_log_returns_annual():
        dates = pd.date_range(start="2000-01-31", periods=36, freq="M")
        return pd.Series(np.linspace(0.01, 0.05, 36), index=dates)
    def dummy_load_ken_french_annual(dataset_name, weighting):
        dates = pd.date_range(start="2000-01-31", periods=36, freq="M")
        data = {
            "Date": dates,
            "A": np.linspace(1, 2, 36),
            "B": np.linspace(2, 3, 36)
        }
        return pd.DataFrame(data)
    
    monkeypatch.setattr(regressions, "load_and_compute_log_returns", dummy_load_and_compute_log_returns_annual)
    monkeypatch.setattr(regressions, "load_ken_french", dummy_load_ken_french_annual)
    dataset_name = "dummy_annual"
    
    forecast_series, actual_series, R2_oos = regressions.run_recursive_forecast_annual(
        dataset_name, weighting="dummy", h=1,
        start_train_year=2000, end_train_year=2001, end_forecast_year=2002, n_components=1
    )
    assert isinstance(forecast_series, pd.Series), "forecast_series is not a Series"
    assert isinstance(actual_series, pd.Series), "actual_series is not a Series"
    assert isinstance(R2_oos, float), "R2_oos is not a float"
