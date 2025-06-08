import warnings
from pathlib import Path
#import xlsxwriter
import pandas as pd
import pandas_datareader.data as web

from settings import config

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


def pull_ken_french_excel(
    dataset_name="Portfolios_Formed_on_INV",
    data_dir=DATA_DIR,
    log=True,
    start_date=START_DATE,
    end_date=END_DATE,
):
    """
    Pulls the Ken French portfolio data..
    
    Parameters:
    - dataset_name (str): Name of the dataset to pull.
    - data_dir (str): Directory to save the Excel file.
    - log (bool): Whether to log the path of the saved Excel file.
    - start_date (str): Start date in 'YYYY-MM-DD' format.
    - end_date (str): End date in 'YYYY-MM-DD' format.
    
    Returns:
    - Excel File: Contains date, return, and other key fields.
    """

    data_dir = Path(data_dir)
    # Suppress the specific FutureWarning about date_parser
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
            message="The argument 'date_parser' is deprecated",
        )
        data = web.DataReader(
            dataset_name,
            "famafrench",
            start=start_date,
            end=end_date,
        )
        excel_path = (
            data_dir / f"{dataset_name.replace('/', '_')}.xlsx"
        )  # Ensure the name is file-path friendly

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Write the description to the first sheet
            if "DESCR" in data:
                description_df = pd.DataFrame([data["DESCR"]], columns=["Description"])
                description_df.to_excel(writer, sheet_name="Description", index=False)

            # Write each table in the data to subsequent sheets
            for table_key, df in data.items():
                if table_key == "DESCR":
                    continue  # Skip the description since it's already handled
                sheet_name = str(table_key)  # Naming sheets by their table_key
                df.to_excel(
                    writer, sheet_name=sheet_name[:31]
                )  # Sheet name limited to 31 characters
    if log:
        print(f"Excel file saved to {excel_path}")
    return excel_path


def load_returns(dataset_name, weighting="value-weighted", data_dir=DATA_DIR):
    data_dir = Path(data_dir)
    excel_path = data_dir / f"{dataset_name.replace('/', '_')}.xlsx"
    if weighting == "value-weighted":
        sheet_name = "0"
    elif weighting == "equal-weighted":
        sheet_name = "1"
    else:
        raise ValueError(f"Invalid weighting: {weighting}")
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    return df


def load_sheet(dataset_name, sheet_name: str = "0", data_dir=DATA_DIR):
    """For example, for dataset_name = '6_Portfolios_2x3V', the full data
    is the description and the 10 tables of returns and properties.

    6 Portfolios 2x3
    ----------------

    This file was created by CMPT_ME_BEME_OP_INV_RETS using the 202412 CRSP database. It contains value- and equal-weighted returns for portfolios formed on ME and BEME. The portfolios are constructed at the end of June. BEME is book value at the last fiscal year end of the prior calendar year divided by ME at the end of December of the prior year. Annual returns are from January to December. Missing data are indicated by -99.99 or -999. The break points include utilities and include financials. The portfolios include utilities and include financials. Copyright 2024 Eugene F. Fama and Kenneth R. French

    0 : Average Value Weighted Returns -- Monthly (1129 rows x 6 cols)
    1 : Average Equal Weighted Returns -- Monthly (1129 rows x 6 cols)
    2 : Average Value Weighted Returns -- Annual (95 rows x 6 cols)
    3 : Average Equal Weighted Returns -- Annual (95 rows x 6 cols)
    4 : Number of Firms in Portfolios (1129 rows x 6 cols)
    5 : Average Market Cap (1129 rows x 6 cols)
    6 : For portfolios formed in June of year t   Value Weight Average of BE/ME Calculated for June of t to June of t+1 as:    Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Dec t-1)] / Sum[ME(Mth)]   Where Mth is a month from June of t to June of t+1   and BE(Fiscal Year t-1) is adjusted for net stock issuance to Dec t-1 (1129 rows x 6 cols)
    7 : For portfolios formed in June of year t   Value Weight Average of BE_FYt-1/ME_June t Calculated for June of t to June of t+1 as:    Sum[ME(Mth) * BE(Fiscal Year t-1) / ME(Jun t)] / Sum[ME(Mth)]   Where Mth is a month from June of t to June of t+1   and BE(Fiscal Year t-1) is adjusted for net stock issuance to Jun t (1129 rows x 6 cols)
    8 : For portfolios formed in June of year t   Value Weight Average of OP Calculated as:    Sum[ME(Mth) * OP(fiscal year t-1) / BE(fiscal year t-1)] / Sum[ME(Mth)]    Where Mth is a month from June of t to June of t+1 (727 rows x 6 cols)
    9 : For portfolios formed in June of year t   Value Weight Average of investment (rate of growth of assets) Calculated as:    Sum[ME(Mth) * Log(ASSET(t-1) / ASSET(t-2) / Sum[ME(Mth)]    Where Mth is a month from June of t to June of t+1 (727 rows x 6 cols)
    """
    
    if isinstance(sheet_name, int):
        sheet_name = str(sheet_name)

    data_dir = Path(data_dir)
    excel_path = data_dir / f"{dataset_name.replace('/', '_')}.xlsx"
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    if sheet_name == "Description":
        return df.iloc[0, 0]
    return df


def _demo():
    df = load_sheet("Portfolios_Formed_on_INV", sheet_name="0")
    df
    df_desc = load_sheet("25_Portfolios_OP_INV_5x5", sheet_name="Description")
    print(df_desc)

    ff_factors = load_sheet("F-F_Research_Data_Factors", sheet_name="0")
    ff_factors
    ff_factors_desc = load_sheet("F-F_Research_Data_Factors", sheet_name="Description")
    print(ff_factors_desc)

    ff_portfolios = load_sheet("6_Portfolios_2x3", sheet_name="0")
    ff_portfolios
    ff_portfolios_desc = load_sheet("6_Portfolios_2x3", sheet_name="Description")
    print(ff_portfolios_desc)


if __name__ == "__main__":
    _ = pull_ken_french_excel(
        dataset_name="6_Portfolios_2x3",
    )  # Save 6_Portfolios_2x3.xlsx
    _ = pull_ken_french_excel(
        dataset_name="25_Portfolios_5x5",
    )  # Save 25_Portfolios_5x5.xlsx
    _ = pull_ken_french_excel(
        dataset_name="100_Portfolios_10x10",
    )  # Save 100_Portfolios_10x10.xlsx
