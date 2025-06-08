import argparse
import os
import boto3
import pandas as pd
import wrds
from pathlib import Path
from settings import config

DATA_DIR = config("DATA_DIR")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--s3_bucket',     required=True)
    p.add_argument('--s3_prefix',     default='')
    p.add_argument('--aws_region',    default='us-east-1')
    p.add_argument('--wrds_username', required=True)
    p.add_argument('--start_date',    required=False, default=None)
    p.add_argument('--end_date',      required=False, default=None)
    args = p.parse_args()

    # inject your username for wrds
    os.environ['WRDS_USERNAME'] = args.wrds_username

    # connect & pull
    db = wrds.Connection(wrds_username=args.wrds_username)
    q = f"""
      SELECT date, vwretd AS value_weighted_return
      FROM crsp.msi
      WHERE date BETWEEN '{args.start_date}' AND '{args.end_date}'
      ORDER BY date
    """ if args.start_date and args.end_date else """
      SELECT date, vwretd AS value_weighted_return
      FROM crsp.msi
      ORDER BY date
    """
    df = db.raw_sql(q, date_cols=['date'])
    db.close()

    # write out
    out_file = Path('crsp_value_weighted_index.parquet')
    df.to_parquet(out_file, index=False)

    # upload
    s3 = boto3.client('s3', region_name=args.aws_region)
    key = f"{args.s3_prefix}{out_file.name}"
    print(f"Uploading → s3://{args.s3_bucket}/{key}")
    s3.upload_file(str(out_file), args.s3_bucket, key)
    print("Done.")
