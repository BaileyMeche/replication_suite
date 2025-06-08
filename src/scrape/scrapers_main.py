#!/usr/bin/env python3
import argparse
import boto3
from settings import config
from s3_utils import ensure_s3_bucket
from launch_scraper_instances import launch_scraper_instances
import time

'''
python scrapers_main.py  --num_instances 4
'''

REGION              = config("REGION")
BUCKET              = config("BUCKET")
LOCAL_MANUAL_DATA_DIR = config("LOCAL_MANUAL_DATA_DIR")
S3_SCRIPTS          = config("S3_SCRIPTS")
S3_MANUAL_DATA      = config("S3_MANUAL_DATA")
USER                = config("USER")
PASSWORD            = config("PASSWORD")
INSTANCE_ROLE       = config("INSTANCE_ROLE")

def wait_for_termination(ec2_client, instance_ids, delay=30):
    while True:
        resp = ec2_client.describe_instances(InstanceIds=instance_ids)
        states = [
            inst['State']['Name']
            for res in resp['Reservations']
            for inst in res['Instances']
        ]
        if all(s == 'terminated' for s in states):
            break
        print(f"Current states {states}, retrying in {delay}s…")
        time.sleep(delay)
    print("All instances terminated.")

if __name__ == '__main__':
    p = argparse.ArgumentParser(
        description="Orchestrate parallel Parquet scrapers onto EC2 → S3"
    )
    p.add_argument('--s3_prefix',       default='',
                   help="Optional key prefix (e.g. 'projectX/')")
    p.add_argument('--ami_id',          default='ami-00a929b66ed6e0de6')
    p.add_argument('--instance_type',   default='t3.micro')
    p.add_argument('--key_name',        default='vockey')
    p.add_argument('--num_instances',   type=int, default=4,
                   help='Should be 4, one per scraper')
    # p.add_argument('--wrds_password',   required=True,
    #                help='Your WRDS password (for CRSP scraper)')
    args = p.parse_args()

    # ensure bucket exists
    ensure_s3_bucket(BUCKET, region=REGION)
    s3 = boto3.client('s3', region_name=REGION)

    # upload each scraper + requirements + settings.py under scripts/
    for fname in (
        'scraper_1_fed_yield_curve.py',
        'scraper_2_fed_tips_yield_curve.py',
        'scraper_3_expectations.py',
        #'scraper_4_pull_CRSP.py',
        'requirements.txt',
        'settings.py'
    ):
        key = f"{S3_SCRIPTS}{fname}"
        print(f"Uploading {fname} → s3://{BUCKET}/{key}")
        s3.upload_file(fname, BUCKET, key)

    # launch EC2 scraper instances, now passing wrds_password
    scraper_ids = launch_scraper_instances(
        s3_bucket=args.BUCKET if False else BUCKET,
        s3_scripts_prefix=S3_SCRIPTS,
        s3_data_prefix=S3_MANUAL_DATA,
        aws_region=REGION,
        ami_id=args.ami_id,
        instance_type=args.instance_type,
        key_name=args.key_name,
        iam_instance_profile=INSTANCE_ROLE,
        num_instances=args.num_instances,
        wrds_username=USER,
        wrds_password=PASSWORD
    )
    # Create an EC2 client and waiter
    ec2 = boto3.client('ec2', region_name=REGION)
    waiter = ec2.get_waiter('instance_terminated')

    print(f"Waiting for EC2 instances {scraper_ids} to terminate…")
    wait_for_termination(ec2, scraper_ids)                          # waits for termination, checks every 30s
    print("All scraper instances terminated. Proceeding with ***")