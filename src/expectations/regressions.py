import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
from load_data import load_crsp_index, load_ken_french, load_and_compute_log_returns
import matplotlib.pyplot as plt
from sklearn.cross_decomposition import PLSRegression
from settings import config
script_dir = os.path.dirname(os.path.abspath(__file__))
save_path = os.path.join(script_dir, "../reports/plots/")  # Correct relative path
os.makedirs(save_path, exist_ok=True)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

def first_stage_regressions(v_df, y_series, h=1):
    """
    For each portfolio (each column in v_df), regress the valuation ratio at time t 
    on the future excess market return y_{t+h} to estimate the sensitivity (phi).
    Returns a dictionary of slope coefficients (phi_i) for each portfolio.
    """
    phi_dict = {}
    for col in v_df.columns:
        # Convert the column to numeric
        v_i = pd.to_numeric(v_df[col], errors='coerce').dropna()
        if v_i.empty:
            continue
        # Align with future excess returns: y_{t+h}
        y_shifted = y_series.shift(-h).loc[v_i.index]
        # Only keep dates where both v_i and y_shifted are available
        valid = v_i.index.intersection(y_shifted.dropna().index)
        #if len(valid) < 10:
            #continue  # Skip if too few observations
        v_i = v_i.loc[valid]
        y_i = y_shifted.loc[valid]
        X = sm.add_constant(y_i)
        try:
            model = sm.OLS(v_i, X).fit()
            # Use positional indexing to get the slope coefficient (position 1)
            phi_dict[col] = model.params.iloc[1]
        except Exception as e:
            print(f"Error in first stage for column {col}: {e}")
    return phi_dict

def second_stage_regressions(v_df, phi_dict):
    """
    For each time t, run a cross-sectional regression of the observed portfolio valuation ratios 
    (v_{i,t}) on the estimated loadings (phi_i). We build a DataFrame for the independent 
    variable so that the regression returns a Series with named coefficients.
    The coefficient on the loadings (named 'phi') is taken as the latent factor F_t.
    """
    F_list = []
    dates = []
    valid_cols = list(phi_dict.keys())
    # Build a DataFrame for the loadings with a constant column.
    X_df = pd.DataFrame({
        'const': 1,
        'phi': [phi_dict[col] for col in valid_cols]
    }, index=valid_cols)
    
    for t, row in v_df[valid_cols].iterrows():
        try:
            # Create a Series for the dependent variable with index matching valid_cols.
            Y = pd.Series(row.values.astype(float), index=valid_cols)
        except Exception as e:
            print(f"Error converting row at time {t} to float: {e}")
            continue
        try:
            model = sm.OLS(Y, X_df).fit()
            # Retrieve the coefficient on 'phi'
            F_t = model.params['phi']
            F_list.append(F_t)
            dates.append(t)
        except Exception as e:
            print(f"Error in second stage regression at time {t}: {e}")
    F_series = pd.Series(F_list, index=dates)
    # Set frequency to match the input v_df if possible.
    try:
        F_series.index = pd.DatetimeIndex(F_series.index).asfreq(v_df.index.freq)
    except Exception:
        pass  # If unable to set frequency, leave as is.
    return F_series


def third_stage_regression(F_series, y_series, h=1):
    """
    Regress the future excess market return (y_{t+h}) on the estimated latent factor F_t.
    The latent factor is renamed to 'F' so that the regression DataFrame has clear column names.
    Returns the OLS regression result.
    """
    y_future = y_series.shift(-h)
    common_dates = F_series.index.intersection(y_future.index)
    F_aligned = F_series.loc[common_dates]
    y_aligned = y_future.loc[common_dates].astype(float)

    # <<< ADD THIS GUARD >>>
    if F_aligned.empty or y_aligned.empty:
        print(f"⚠️  Skipping third‐stage regression for h={h}: no overlapping dates")
        return None

    X = sm.add_constant(F_aligned)
    model = sm.OLS(y_aligned, X).fit()
    return model

def run_in_sample_pls(dataset_name, weighting="value-weighted", h=1, end_date=None):
    """
    Runs the in-sample three-stage PLS procedure:
      1. Loads and aligns excess market returns (market return minus TB3MS) and Ken French portfolio data.
      2. Computes first-stage loadings for each portfolio.
      3. Extracts the latent factor F_t via cross-sectional regressions.
      4. Runs the predictive regression of excess return y_{t+h} on F_t.
    
    Returns a dictionary with intermediate results.
    """
    # Compute excess returns
    y_excess = load_and_compute_log_returns()
    
    # Load Ken French portfolio data
    ken_df = load_ken_french(dataset_name, weighting)
    if "Date" in ken_df.columns:
        ken_df["Date"] = pd.to_datetime(ken_df["Date"])
        ken_df = ken_df.set_index("Date")
    v_df = ken_df.apply(pd.to_numeric, errors='coerce')
    
    # Align datasets by common dates
    common_dates = v_df.index.intersection(y_excess.index)
    if common_dates.empty:
        raise ValueError("No common dates found between Ken French data and excess returns.")
    v_df = v_df.loc[common_dates]
    y_excess = y_excess.loc[common_dates]
    
    print(f"Aligned data from {common_dates.min().date()} to {common_dates.max().date()}")
    print(f"Number of dates: {len(common_dates)}")

    y_excess = y_excess[y_excess.index <= end_date]
    v_df = v_df[v_df.index <= end_date]

    print(f"Date range: {y_excess.index.min().date()} to {y_excess.index.max().date()}")
    print(f"Number of dates: {len(y_excess)}")
    print(f"Date range: {v_df.index.min().date()} to {v_df.index.max().date()}")
    print(f"Number of dates: {len(v_df)}")
    
    # First Stage
    phi_dict = first_stage_regressions(v_df, y_excess, h)
    print("First-Stage Estimated Loadings (phi_i):")
    if not phi_dict:
        print("No valid loadings estimated. Check your valuation ratio data.")
    else:
        for key, value in phi_dict.items():
            print(f"{key}: {value:.4f}")
    print("First stage completed.")
    
    # Second Stage
    F_series = second_stage_regressions(v_df, phi_dict)
    print("\nEstimated Latent Factor (F_t) Sample:")
    print(F_series.head())
    print("Second stage completed.")
    
    # Third Stage
    third_model = third_stage_regression(F_series, y_excess, h)
    if third_model is not None:
        print("\nThird-Stage Regression Summary:")
        print(third_model.summary())
        # safe to pull the stats
        r_squared     = f"{third_model.rsquared:.6f}"
        r_squared_adj = f"{third_model.rsquared_adj:.6f}"
    else:
        print(f"⚠️  Skipping third-stage regression for h={h}: no overlapping dates")
        # use None or some default
        r_squared     = None
        r_squared_adj = None

    return {
        "phi": phi_dict,
        "F_series": F_series,
        "third_model": third_model,
        "v_df": v_df,
        "y_excess": y_excess
    }

def run_recursive_forecast(
    dataset_name, 
    weighting="value-weighted", 
    h=1, 
    start_train_date='1930-01-01', 
    end_train_date='1980-01-01',
    end_forecast_date='2011-01-01'
):
    """
    Implements a recursive (rolling-window) out-of-sample forecasting procedure.
    For each forecast date (after the training period), re-estimates the model using data 
    available up to that point, and generates a forecast for y_{t+h} (excess return).
    
    Returns a dictionary containing forecasted series, actual excess returns, and the out-of-sample R².
    """
    # Compute excess returns
    y_excess = load_and_compute_log_returns()
    
    ken_df = load_ken_french(dataset_name, weighting)
    if "Date" in ken_df.columns:
        ken_df["Date"] = pd.to_datetime(ken_df["Date"])
        ken_df = ken_df.set_index("Date")
    v_df = ken_df.apply(pd.to_numeric, errors='coerce')
    
    common_dates = v_df.index.intersection(y_excess.index).sort_values()
    v_df = v_df.loc[common_dates]
    y_excess = y_excess.loc[common_dates]

    v_df = v_df[v_df.index <= end_forecast_date]
    y_excess = y_excess[y_excess.index <= end_forecast_date]

    train_end = pd.to_datetime(end_train_date)

    print(f"Start training date: {start_train_date}")
    print(f"End training date: {train_end}")
    print(f"End forecast date: {end_forecast_date}")
    
    forecast_dates = v_df.loc[train_end:].index
    forecasts = {}
    actuals = {}
    
    for forecast_date in forecast_dates:
        train_idx = v_df.index < forecast_date
        v_train = v_df.loc[train_idx]
        y_train = y_excess.loc[train_idx]
        #if len(v_train) < 30:
            #continue
        phi_dict = first_stage_regressions(v_train, y_train, h)
        if not phi_dict:
            continue
        F_train = second_stage_regressions(v_train, phi_dict)
        if F_train.empty:
            continue
        F_forecast = F_train.iloc[-1]
        third_model = third_stage_regression(F_train, y_train, h)
        # determine forecast_value in all cases
        if third_model is None or 'F' not in third_model.params:
            logger.warning(
                f"Skipping forecast for {forecast_date}: no 'F' coefficient found."
            )
            forecast_value = np.nan
        else:
            forecast_value = (
                third_model.params['const']
                + third_model.params['F'] * F_forecast
            )
        forecasts[forecast_date] = forecast_value
        if forecast_date in y_excess.index:
            actuals[forecast_date] = y_excess.loc[forecast_date]
    
    forecast_series = pd.Series(forecasts)
    actual_series = pd.Series(actuals)
    common = forecast_series.index.intersection(actual_series.index)
    forecast_series = forecast_series.loc[common]
    actual_series = actual_series.loc[common]
    
    mean_y = y_excess.mean()
    ss_res = np.sum((actual_series - forecast_series) ** 2)
    ss_tot = np.sum((actual_series - mean_y) ** 2)
    R2_oos = 1 - ss_res / ss_tot if ss_tot != 0 else np.nan
    
    return {
        "forecast_series": forecast_series,
        "actual_series": actual_series,
        "R2_oos": R2_oos
    }

def display_results(dataset_label, in_sample_results, recursive_results):
    """
    Displays the in-sample regression summary, out-of-sample R²,
    and saves the monthly actual vs. forecasted excess market returns plot.
    """
    print(f"\n{'='*40}\nResults for {dataset_label} Portfolio Data\n{'='*40}")
    print("\nThird-Stage In-Sample Regression Summary:")
    print(in_sample_results["third_model"].summary())
    print(f"\nOut-of-Sample Predictive R²: {recursive_results['R2_oos']:.4f}")

    plt.figure(figsize=(10, 6))
    plt.plot(recursive_results["actual_series"], label="Actual Excess Market Return")
    plt.plot(recursive_results["forecast_series"], label="Forecasted Excess Market Return", linestyle="--")
    plt.title(f"Monthly Out-of-Sample Forecasts for {dataset_label} Portfolio Data")
    plt.xlabel("Date")
    plt.ylabel("Excess Market Return")
    plt.legend()

    # Save the plot with "monthly" in the filename
    dataset_label_clean = dataset_label.replace(" ", "_").replace("-", "_")
    filename = f"Monthly_Out_of_Sample_Forecasts_for_{dataset_label_clean}_Portfolio_Data.png"
    plt.savefig(os.path.join(save_path, filename), dpi=300, bbox_inches='tight')
    plt.show()

#############################
# Helper Function to Select Annual Predictor Snapshot
#############################
def select_predictors_annual(X, target_month=6):
    """
    For each year in the monthly predictor DataFrame X, select the observation corresponding 
    to a specific month (default is June, target_month=6) as used in many asset pricing studies.
    If an observation for the target month is not available, select the last available observation of that year.
    
    Returns:
        DataFrame with index reset to January 1 of each year.
    """
    # Ensure the DataFrame index is datetime.
    if not isinstance(X.index, pd.DatetimeIndex):
        X.index = pd.to_datetime(X.index)
    
    annual_list = []
    for year, group in X.groupby(X.index.year):
        # Select observation where month equals target_month
        target_obs = group[group.index.month == target_month]
        if not target_obs.empty:
            chosen = target_obs.iloc[0]
        else:
            # If no target month observation, take the last available observation
            chosen = group.iloc[-1]
        # Reset the index for the chosen observation to January 1 of that year
        chosen.name = pd.Timestamp(year=year, month=1, day=1)
        annual_list.append(chosen)
    annual_X = pd.DataFrame(annual_list)
    return annual_X


#############################
# Helper function to aggregate monthly log returns to annual log returns
#############################
def aggregate_to_annual_returns(returns_series):
    """
    Compute rolling 12-month log returns.
    Since log returns are additive, the 12-month return is computed as the sum over a rolling window of 12 months.
    Then, shift the series by -12 so that each observation represents the cumulative return from t+1 to t+12.
    
    Returns:
        Series of rolling 12-month log returns.
    """
    rolling_annual = returns_series.rolling(window=12).sum()
    rolling_annual = rolling_annual.shift(-12)
    return rolling_annual.dropna()

#############################
# Annual In-Sample and Recursive Forecasting Functions
#############################

def run_in_sample_pls_annual(dataset_name, weighting="BE_FYt-1_to_ME_June_t", h=1, end_date='1980-01-01'):
    """
    Runs the in-sample three-stage PLS procedure on annualized data.
      - Aggregates monthly log returns to annual log returns (by summing).
      - Selects a single snapshot for predictors from each year (using the June observation if available).
      - Uses h=1 on the annual series (i.e., forecasting next year's log return).

    Returns:
        dict: A dictionary with intermediate results including:
            - phi: Dictionary of first-stage estimated loadings.
            - F_series: Series of estimated latent factors.
            - third_model: The OLS regression result from the third stage.
            - X: The aligned predictors DataFrame.
            - y_excess: The aligned annual log returns Series.
    """
    # Compute monthly log returns and aggregate to annual
    monthly_excess = load_and_compute_log_returns()
    annual_excess = aggregate_to_annual_returns(monthly_excess)
    # Convert annual_excess index to annual frequency (set to January 1 of each year)
    annual_excess.index = pd.to_datetime(annual_excess.index).to_period('Y').to_timestamp()
    # Group by the annual index to ensure one unique observation per year.
    annual_excess = annual_excess.groupby(annual_excess.index).first()
    
    # Load monthly predictor data and select a snapshot for each year.
    ken_df = load_ken_french(dataset_name, weighting)
    if "Date" in ken_df.columns:
        ken_df["Date"] = pd.to_datetime(ken_df["Date"])
        ken_df = ken_df.set_index("Date")
    X_monthly = ken_df.apply(pd.to_numeric, errors='coerce')
    annual_X = select_predictors_annual(X_monthly, target_month=1)
    
    # Align data on annual frequency and restrict to in-sample period
    common_years = annual_X.index.intersection(annual_excess.index)
    if common_years.empty:
        raise ValueError("No common years found between predictors and log returns.")
    # Subset both predictor and return series to the common years
    annual_X = annual_X.loc[common_years]
    annual_excess = annual_excess.loc[common_years]
    
    print(f"[Annual In-Sample] Data from {annual_X.index.min().date()} to {annual_X.index.max().date()} (n = {len(annual_X)})")
    annual_X = annual_X.loc[annual_X.index <= pd.to_datetime(end_date)]
    annual_excess = annual_excess.loc[annual_excess.index <= pd.to_datetime(end_date)]
    
    print(f"[Annual In-Sample] Data after end_date filtering: predictors from {annual_X.index.min().date()} to {annual_X.index.max().date()} (n = {len(annual_X)})")
    print(f"[Annual In-Sample] Log returns from {annual_excess.index.min().date()} to {annual_excess.index.max().date()} (n = {len(annual_excess)})")
    
    # First Stage
    phi_dict = first_stage_regressions(annual_X, annual_excess, h)
    print("Annual First-Stage Estimated Loadings (phi_i):")
    for key, value in phi_dict.items():
        print(f"{key}: {value:.4f}")
    
    # Second Stage
    F_series = second_stage_regressions(annual_X, phi_dict)
    print("\nAnnual Estimated Latent Factor (F_t) Sample:")
    print(F_series.head())
    
    # Third Stage
    third_model = third_stage_regression(F_series, annual_excess, h)
    print("\n[Annual In-Sample] Third-Stage Regression Summary:")
    print(third_model.summary())
    
    return {
        "phi": phi_dict,
        "F_series": F_series,
        "third_model": third_model,
        "X": annual_X,
        "y_excess": annual_excess
    }

def run_recursive_forecast_annual(
    dataset_name, 
    weighting="BE_FYt-1_to_ME_June_t", 
    h=1, 
    start_train_year=1930, 
    end_train_year=1979,
    end_forecast_year=2010,
    n_components=1
):
    """
    Implements a recursive (expanding window) out-of-sample forecasting procedure on annual data.
    
    For each forecast year from end_train_year+1 to end_forecast_year:
      1. Uses all annual data up to that year as the training sample.
      2. Fits the PLSRegression on the training data and extracts the PLS factor.
      3. Runs a predictive OLS regression on the training window.
      4. Uses the last observation of the PLS factor to forecast next year's log return.
    
    Returns:
        A tuple (forecast_series, actual_series, R2_oos) for the out-of-sample period.
    """
    monthly_excess = load_and_compute_log_returns()
    annual_excess = aggregate_to_annual_returns(monthly_excess)
    
    ken_df = load_ken_french(dataset_name, weighting)
    if "Date" in ken_df.columns:
        ken_df["Date"] = pd.to_datetime(ken_df["Date"])
        ken_df = ken_df.set_index("Date")
    X_monthly = ken_df.apply(pd.to_numeric, errors='coerce')
    annual_X = select_predictors_annual(X_monthly, target_month=1)
    
    common_years = annual_X.index.intersection(annual_excess.index)
    annual_X = annual_X.loc[common_years]
    annual_excess = annual_excess.loc[common_years]
    
    start_train_year = int(start_train_year)
    end_train_year = int(end_train_year)
    end_forecast_year = int(end_forecast_year)
    
    start_train_date = pd.to_datetime(f"{start_train_year}-01-01")
    end_train_date = pd.to_datetime(f"{end_train_year}-01-01")
    end_forecast_date = pd.to_datetime(f"{end_forecast_year}-01-01")
    
    print(f"[Annual Recursive] Training from {start_train_date.date()} to {end_train_date.date()}, forecasting from {end_train_date.date()} to {end_forecast_date.date()}")
    
    forecast_years = annual_X.loc[end_train_date:].index
    forecasts = {}
    actuals = {}
    
    for forecast_date in forecast_years:
        train_idx = annual_X.index < forecast_date
        X_train = annual_X.loc[train_idx]
        y_train = annual_excess.loc[train_idx]
        if len(X_train) < 3:
            continue
        pls = PLSRegression(n_components=n_components)
        pls.fit(X_train, y_train)
        factor_train = pls.transform(X_train)
        if n_components == 1:
            factor_train = factor_train.flatten()
            factor_train = pd.DataFrame({'F': factor_train}, index=X_train.index)
        else:
            factor_train = pd.DataFrame(factor_train, index=X_train.index, columns=[f"F{i+1}" for i in range(n_components)])
        
        X_train_const = sm.add_constant(factor_train)
        model = sm.OLS(y_train, X_train_const).fit()
        
        factor_forecast = pls.transform(X_train.iloc[[-1]])
        if n_components == 1:
            factor_forecast = factor_forecast.flatten()[0]
            factor_forecast_df = pd.DataFrame({'F': [factor_forecast]}, index=[X_train.index[-1]])
        else:
            factor_forecast_df = pd.DataFrame(factor_forecast, index=[X_train.index[-1]], columns=[f"F{i+1}" for i in range(n_components)])
        
        X_curr = sm.add_constant(factor_forecast_df, has_constant='add')
        forecast_value = model.predict(X_curr)[0]
        forecasts[forecast_date] = forecast_value
        if forecast_date in annual_excess.index:
            actuals[forecast_date] = annual_excess.loc[forecast_date]
    
    forecast_series = pd.Series(forecasts)
    actual_series = pd.Series(actuals)
    common = forecast_series.index.intersection(actual_series.index)
    forecast_series = forecast_series.loc[common]
    actual_series = actual_series.loc[common]
    
    mean_y = annual_excess.mean()
    ss_res = np.sum((actual_series - forecast_series) ** 2)
    ss_tot = np.sum((actual_series - mean_y) ** 2)
    R2_oos = 1 - ss_res / ss_tot if ss_tot != 0 else np.nan
    
    return forecast_series, actual_series, R2_oos


def display_results_annual(dataset_label, in_sample_results, forecast_series, actual_series, R2_oos):
    """
    Displays the in-sample OLS regression summary (annual) from the PLS predictor,
    out-of-sample R², and saves the annual actual vs. forecasted log returns plot.
    """
    print(f"\n{'='*40}\n[Annual] Results for {dataset_label} Portfolio Data\n{'='*40}")
    print("\n[Annual] Third-Stage In-Sample Regression Summary:")
    print(in_sample_results["third_model"].summary())
    print(f"\n[Annual] Out-of-Sample Predictive R²: {R2_oos:.4f}")

    plt.figure(figsize=(10, 6))
    plt.plot(actual_series, label="Actual Annual Log Return")
    plt.plot(forecast_series, label="Forecasted Annual Log Return", linestyle="--")
    plt.title(f"Annual Out-of-Sample Forecasts for {dataset_label} Portfolio Data")
    plt.xlabel("Year")
    plt.ylabel("Annual Log Return")
    plt.legend()

    # Save the plot with "annual" in the filename
    dataset_label_clean = dataset_label.replace(" ", "_").replace("-", "_")
    filename = f"Annual_Out_of_Sample_Forecasts_for_{dataset_label_clean}_Portfolio_Data.png"
    plt.savefig(os.path.join(save_path, filename), dpi=300, bbox_inches='tight')
    plt.show()
