import boto3


def launch_scraper_instances(
    s3_bucket,
    s3_scripts_prefix,
    s3_data_prefix,
    aws_region,
    ami_id,
    instance_type,
    key_name,
    iam_instance_profile,
    num_instances,
    wrds_username,
    wrds_password
):
    ec2 = boto3.client('ec2', region_name=aws_region)
    instance_ids = []

    # Four scraper scripts
    scrapers = [
        'scraper_1_fed_yield_curve.py',
        'scraper_2_fed_tips_yield_curve.py',
        'scraper_3_expectations.py'
        #'scraper_4_pull_CRSP.py',
    ]

    for i, script in enumerate(scrapers, start=1):
        print(f"Launching instance #{i} for {script}…")

        # Determine dependencies
        if script == 'scraper_3_expectations.py':
            deps = 'pandas pandas_datareader xlsxwriter openpyxl pyarrow boto3 python-decouple'
        elif script == 'scraper_4_pull_CRSP.py':
            deps = 'pandas wrds pyarrow boto3 python-decouple'
        else:
            deps = 'pandas requests pyarrow boto3 python-decouple'

        user_data = f"""#!/bin/bash
set -e
cd /home/ec2-user

# 1) Setup virtualenv
python3 -m venv env
source env/bin/activate
pip install --upgrade pip

# 2) Install dependencies
pip install {deps}

# 3) Download settings.py and the scraper
aws s3 cp s3://{s3_bucket}/{s3_scripts_prefix}settings.py   ./settings.py
aws s3 cp s3://{s3_bucket}/{s3_scripts_prefix}{script}     ./{script}

# 4) If CRSP scraper, export both WRDS creds
if [[ "{script}" == "scraper_4_pull_CRSP.py" ]]; then
  export WRDS_USERNAME={wrds_username}
  export WRDS_PASSWORD={wrds_password}
fi

# 5) Execute the scraper
if [[ "{script}" == "scraper_3_expectations.py" ]]; then
  python3 {script} \
    --s3_bucket {s3_bucket} \
    --s3_prefix {s3_data_prefix} \
    --aws_region {aws_region} \
    --start_date 1913-01-01 \
    --end_date 2024-01-01
elif [[ "{script}" == "scraper_4_pull_CRSP.py" ]]; then
  python3 {script} \
    --s3_bucket {s3_bucket} \
    --s3_prefix {s3_data_prefix} \
    --aws_region {aws_region} \
    --wrds_username {wrds_username} \
    --start_date 1913-01-01 \
    --end_date 2024-01-01
else
  python3 {script} \
    --s3_bucket {s3_bucket} \
    --s3_prefix {s3_data_prefix} \
    --aws_region {aws_region}
fi

# 6) On success, terminate
shutdown -h now
"""

        resp = ec2.run_instances(
            ImageId=ami_id,
            InstanceType=instance_type,
            KeyName=key_name,
            IamInstanceProfile={'Name': iam_instance_profile},
            MinCount=1,
            MaxCount=1,
            UserData=user_data,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Purpose', 'Value': 'ParquetScraper'},
                    {'Key': 'Script',  'Value': script}
                ]
            }],
            InstanceInitiatedShutdownBehavior='terminate'
        )
        # Extract and store the new instance's ID
        instance_id = resp['Instances'][0]['InstanceId']
        instance_ids.append(instance_id)

    print(f"\nLaunched {num_instances} scraper instances.")
    return instance_ids