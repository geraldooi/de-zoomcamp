import os
import requests
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from typing import List


@task(retries=3)
def fetch(dataset_url: str, color: str) -> str:
    """Download file from the url given and return the file path"""

    # Fetch the dataset from the url
    response = requests.get(dataset_url)

    # Create the output directory if not exists
    output_dir = f"./data/{color}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # Save the dataset into file
    filename = Path(f"{output_dir}/{os.path.basename(dataset_url)}")
    with open(filename, "wb") as f:
        f.write(response.content)
    
    return filename


@task(log_prints=True)
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

    write_gcs(fetch(dataset_url, color))


@flow
def etl_web_to_gcs_parent(color: str = "fhv", year: int = 2019, months: List[int] = [1, 2, 3]) -> None:
    """Parent ETL Flow"""

    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    color = "fhv"
    year = 2019
    months = [i for i in range(1, 13)]
    etl_web_to_gcs_parent(color, year, months)