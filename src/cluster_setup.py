# src/cluster_setup.py

from dask_cloudprovider.aws import EC2Cluster
from dask.distributed     import Client
import configparser, os, contextlib, re, dask, subprocess, sys, time, boto3
from platform             import python_version
from pathlib import Path

def pin_versions():
    """
    Ensure we have exactly botocore 1.36.3, aiobotocore 2.19.0 installed.
    """
    pkgs = [
        "botocore==1.36.3",
        "aiobotocore==2.19.0",
    ]
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade"] + pkgs
    )

def get_aws_credentials():
    """Read in your AWS config/credentials and return them as env vars."""
    parser = configparser.RawConfigParser()
    parser.read(os.path.expanduser("~/.aws/config"))
    cfg = dict(parser.items("default"))
    parser.read(os.path.expanduser("~/.aws/credentials"))
    creds = dict(parser.items("default"))
    all_ = {k.upper(): v for k, v in {**cfg, **creds}.items()}
    # rename region if needed
    if "REGION" in all_:
        all_["AWS_REGION"] = all_.pop("REGION")
    return all_

def start_cluster(n_workers=4, instance_type="c7a.large", security=False):
    """
    1) pins versions
    2) reads aws creds
    3) launches an EC2Cluster
    4) waits for all workers to connect
    5) writes scheduler address to ENV + file
    returns path to scheduler file
    """
    pin_versions()
    env_vars = get_aws_credentials()
    env_vars["EXTRA_PIP_PACKAGES"] = "s3fs"

    # pick a matching Dask docker image
    py_v = "-py" + re.findall(r"\d\.\d+", python_version())[0]
    docker = f"daskdev/dask:{dask.__version__ + py_v}"
    print("--Using Docker image:", docker)

    # launch
    cluster = EC2Cluster(
        instance_type=instance_type,
        n_workers=n_workers,
        security=security,
        docker_image=docker,
        env_vars=env_vars
    )
    client = Client(cluster)

    # wait until all workers are up (timeout=300s)
    client.wait_for_workers(n_workers, timeout=300)
    print(f"{n_workers} workers connected")

    # grab the scheduler address
    sched_addr = cluster.scheduler_address
    print("Scheduler at:", sched_addr)
    env_path = Path("../.") / ".env"
    if not env_path.exists():
        env_path.write_text("")  # make sure file exists

    # read existing lines, dropping any old DASK_SCHEDULER_ADDRESS
    lines = []
    with env_path.open("r") as f:
        for line in f:
            if not line.startswith("DASK_SCHEDULER_ADDRESS="):
                lines.append(line)

    # append the new setting
    lines.append(f"DASK_SCHEDULER_ADDRESS={sched_addr}\n")

    # write back
    with env_path.open("w") as f:
        f.writelines(lines)

    # write a scheduler file for other processes to pick up
    sched_file = os.path.abspath("dask-scheduler.json")
    client.write_scheduler_file(sched_file)
    print("Wrote scheduler file →", sched_file)

    return sched_file
