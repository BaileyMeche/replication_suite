from settings import config
import pandas as pd
import requests
from io import BytesIO
from pathlib import Path

import argparse
import boto3

DATA_DIR = config("DATA_DIR")


# Define the URL for the TIPS yield data
TIPS_URL = "https://www.federalreserve.gov/data/yield-curve-tables/feds200805.csv"

def pull_fed_tips_yield_curve():
    """
    Download and process the latest zero-coupon TIPS yield curve from the Federal Reserve.

    Expected CSV structure:
    - Metadata in the first 19 rows (to be skipped).
    - 'Date' column in YMD format.
    - TIPS yield columns named 'TIPSY02', 'TIPSY05', 'TIPSY10', 'TIPSY20'.
    - Yield values are in percentage terms and must be converted to decimals.

    Returns:
        pd.DataFrame: Processed TIPS yield data.
    """
    # Fetch the data from the Federal Reserve
    response = requests.get(TIPS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch TIPS data: HTTP {response.status_code}")
    
    # Read CSV while skipping the first 19 rows (metadata)
    df = pd.read_csv(BytesIO(response.content), skiprows=18)
    
    return df


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--s3_bucket',   required=True)
    p.add_argument('--s3_prefix',   default='')
    p.add_argument('--aws_region',  default='us-east-1')
    args = p.parse_args()

    df = pull_fed_tips_yield_curve()
    rename_map = {
        'TIPSY02': 'TIPS_Treasury_02Y',
        'TIPSY05': 'TIPS_Treasury_05Y',
        'TIPSY10': 'TIPS_Treasury_10Y',
        'TIPSY20': 'TIPS_Treasury_20Y',
    }
    df2 = df.rename(columns=rename_map)

    out_file = Path('fed_tips_yield_curve.parquet')
    df2.to_parquet(out_file, index=False)

    s3 = boto3.client('s3', region_name=args.aws_region)
    key = f"{args.s3_prefix}fed_tips_yield_curve.parquet"
    print(f"Uploading → s3://{args.s3_bucket}/{key}")
    s3.upload_file(str(out_file), args.s3_bucket, key)
    print("Done.")
