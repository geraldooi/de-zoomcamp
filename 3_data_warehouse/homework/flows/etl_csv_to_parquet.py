import os
import requests
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from typing import List


@task(retries=3)
def transform_file(path: Path) -> Path:
    """Load the file into DataFrame then convert it into Parquet file"""

    df = pd.read_csv(path, compression="gzip")

    filename = str(path.name).split(".")[0]
    color = str(path.parent).split("/")[-1]

    # Create the output directory if not exists
    output_dir = f"./data/{color}_parquet"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    new_path = Path(f"{output_dir}/{filename}.parquet")

    df.to_parquet(new_path, compression="gzip")

    return new_path


@task(log_prints=True, retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-de-zoomcamp-375108")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=path
    )


@flow()
def etl_csv_to_parquet(color: str = "fhv", year: int = 2019, month: int = 1) -> None:
    """The main ETL function"""

    dataset_file = Path(f"data/{color}/{color}_tripdata_{year}-{month:02}.csv.gz")

    write_gcs(transform_file(dataset_file))


@flow
def etl_csv_to_parquet_parent(color: str = "fhv", year: int = 2019, months: List[int] = [1, 2, 3]) -> None:
    """Parent ETL Flow"""

    for month in months:
        etl_csv_to_parquet(color, year, month)


if __name__ == "__main__":
    color = "fhv"
    year = 2019
    months = [i for i in range(1, 13)]
    etl_csv_to_parquet_parent(color, year, months)