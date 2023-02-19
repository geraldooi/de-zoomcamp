import os
import requests
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from typing import List


class ParseDF:
    def __init__(self, color: str) -> None:
        if color == "yellow":
            self.dtype = {
                'VendorID': 'string',
                'passenger_count': 'Int64',
                'trip_distance': 'Float64',
                'RatecodeID': 'string',
                'store_and_fwd_flag': 'string',
                'PULocationID': 'Int64',
                'DOLocationID': 'Int64',
                'payment_type': 'Int64',
                'fare_amount': 'Float64',
                'extra': 'Float64',
                'mta_tax': 'Float64',
                'tip_amount': 'Float64',
                'tolls_amount': 'Float64',
                'improvement_surcharge': 'Float64',
                'total_amount': 'Float64',
                'congestion_surcharge': 'Float64'
            }
            self.parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        elif color == "green":
            self.dtype = {
                'VendorID': 'string',
                'store_and_fwd_flag': 'string',
                'RatecodeID': 'Int64',
                'PULocationID': 'Int64',
                'DOLocationID': 'Int64',
                'passenger_count': 'Int64',
                'trip_distance': 'Float64',
                'fare_amount': 'Float64',
                'extra': 'Float64',
                'mta_tax': 'Float64',
                'tip_amount': 'Float64',
                'tolls_amount': 'Float64',
                'ehail_fee': 'Float64',
                'improvement_surcharge': 'Float64',
                'total_amount': 'Float64',
                'payment_type': 'Int64',
                'trip_type': 'Int64',
                'congestion_surcharge': 'Float64'
            }
            self.parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
        elif color == "fhv":
            self.dtype = {
                'dispatching_base_num': 'string',
                'PUlocationID': 'Int64',
                'DOlocationID': 'Int64',
                'SR_Flag': 'Int64',
                'Affiliated_base_number': 'string'
            }
            self.parse_dates = ["pickup_datetime", "dropOff_datetime"]


@task(retries=3)
def fetch(dataset_url: str, color: str) -> str:
    """
    Download file from the url given and return the file path
    """
    output_dir = f"./data/{color}"
    dataset_name = os.path.basename(dataset_url)
    dataset_path = Path(f"{output_dir}/{dataset_name}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    dataset_content = requests.get(dataset_url).content

    with open(dataset_path, "wb") as f:
        f.write(dataset_content)
    
    return dataset_path


@task()
def transform(path: Path) -> None:
    """
    Transform data into parquet
    """
    dataset_name = str(path.name).split(".")[0]
    color = str(path.parent).split("/")[-1]
    output_dir = f"./data/{color}_parquet"
    new_dataset_path = Path(f"{output_dir}/{dataset_name}.parquet")
    df_read_csv_args = ParseDF(color)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(
        path, 
        parse_dates=df_read_csv_args.parse_dates,
        dtype=df_read_csv_args.dtype
    )
    df.to_parquet(new_dataset_path, compression="gzip")

    return new_dataset_path


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-de-zoomcamp-375108")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=path
    )


@flow()
def etl_web_to_gcs(color: str = "fhv", year: int = 2019, month: int = 1) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # write_gcs(transform(fetch(dataset_url, color)))
    write_gcs(Path(f"./data/{color}_parquet/{dataset_file}.parquet"))


@flow
def etl_web_to_gcs_parent(color: str = "fhv", year: int = 2019, months: List[int] = [i+1 for i in range(12)]) -> None:
    """Parent ETL Flow"""

    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    etl_web_to_gcs_parent("yellow", 2019)
    etl_web_to_gcs_parent("yellow", 2020)
    etl_web_to_gcs_parent("green", 2019)
    etl_web_to_gcs_parent("green", 2020)
    etl_web_to_gcs_parent("fhv", 2019)