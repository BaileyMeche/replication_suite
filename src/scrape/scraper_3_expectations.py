import warnings
from pathlib import Path
import pandas as pd
import pandas_datareader.data as web
from settings import config
import argparse
import boto3
from pathlib import Path

'''
TODO: PARALLELIZE THIS FURTHER 
'''

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

        with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
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

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--s3_bucket',   required=True)
    p.add_argument('--s3_prefix',   default='')
    p.add_argument('--aws_region',  default='us-east-1')
    p.add_argument('--start_date',  required=True,
                   help='YYYY-MM-DD')
    p.add_argument('--end_date',    required=True)
    args = p.parse_args()

    s3 = boto3.client('s3', region_name=args.aws_region)
    datasets = ["6_Portfolios_2x3", "25_Portfolios_5x5", "100_Portfolios_10x10"]

    for ds in datasets:
        # 1) download Excel
        excel_path = pull_ken_french_excel(
            dataset_name=ds,
            data_dir=Path('.'),
            log=False,
            start_date=args.start_date,
            end_date=args.end_date
        )

        # 2) explode into Parquet per sheet
        xls = pd.ExcelFile(excel_path)
        safe = ds.replace('/', '_')
        for sheet in xls.sheet_names:
            if sheet.lower() == 'description':
                continue
            df = pd.read_excel(xls, sheet_name=sheet)
            out_file = Path(f"{safe}_sheet{sheet}.parquet")
            df.to_parquet(out_file, index=False)

            key = f"{args.s3_prefix}{out_file.name}"
            print(f"Uploading → s3://{args.s3_bucket}/{key}")
            s3.upload_file(str(out_file), args.s3_bucket, key)

    print("All Fama–French tables uploaded.")
