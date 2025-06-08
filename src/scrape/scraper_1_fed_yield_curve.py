import pandas as pd
import requests
from io import BytesIO
from pathlib import Path
import argparse
import boto3
from settings import config
DATA_DIR = config('DATA_DIR')

def pull_fed_yield_curve():
    """
    Download the latest yield curve from the Federal Reserve
    
    This is the published data using Gurkaynak, Sack, and Wright (2007) model
    
    load in as: 
    "Treasury_SF_10Y",
    "Treasury_SF_02Y",
    "Treasury_SF_20Y",
    "Treasury_SF_03Y",
    "Treasury_SF_30Y",
    "Treasury_SF_05Y",
    """
    
    url = "https://www.federalreserve.gov/data/yield-curve-tables/feds200628.csv"
    response = requests.get(url)
    pdf_stream = BytesIO(response.content)
    df_all = pd.read_csv(pdf_stream, skiprows=9, index_col=0, parse_dates=True)

    cols = ['SVENY' + str(i).zfill(2) for i in range(1, 31)]
    df = df_all[cols]
    return df_all, df

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--s3_bucket',   required=True)
    p.add_argument('--s3_prefix',   default='')
    p.add_argument('--aws_region',  default='us-east-1')
    args = p.parse_args()

    # fetch & select+rename
    df_all, df = pull_fed_yield_curve()
    rename_map = {
        'SVENY02': 'Treasury_SF_02Y',
        'SVENY03': 'Treasury_SF_03Y',
        'SVENY05': 'Treasury_SF_05Y',
        'SVENY10': 'Treasury_SF_10Y',
        'SVENY20': 'Treasury_SF_20Y',
        'SVENY30': 'Treasury_SF_30Y',
    }
    df2 = df.rename(columns=rename_map)

    # write local Parquet
    out_file = Path('fed_yield_curve.parquet')
    df2.to_parquet(out_file, index=False)

    # upload to S3
    s3 = boto3.client('s3', region_name=args.aws_region)
    key = f"{args.s3_prefix}fed_yield_curve.parquet"
    print(f"Uploading → s3://{args.s3_bucket}/{key}")
    s3.upload_file(str(out_file), args.s3_bucket, key)
    print("Done.")
