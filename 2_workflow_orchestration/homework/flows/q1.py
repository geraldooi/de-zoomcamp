import os
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""

    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe our locally as parquet file"""

    # Create directory if not exists
    output_dir = f"./data/{color}"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    path = Path(f"{output_dir}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to gcs"""

    color = str(path.parent).split('/')[-1]

    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-de-zoomcamp-375108")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=path,
        to_path=f"data/{color}/{path.name}"
    )


@flow()
def etl_web_to_gcs(color: str = "green", year: int = 2020, month: int = 1) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    color = "green"
    year = 2020
    month = 1
    etl_web_to_gcs(color, year, month)