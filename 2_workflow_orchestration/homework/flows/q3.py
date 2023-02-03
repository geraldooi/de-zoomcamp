import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect.deployments import Deployment
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from typing import List


@task(retries=3)
def extract_from_gcs(color: str, year: str, month: str) -> pd.DataFrame:
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_cloud_storage_bucket_block = GcsBucket.load("prefect-de-zoomcamp-375108")
    gcp_cloud_storage_bucket_block.get_directory(
        from_path=gcs_path,
        local_path=f"gcs_data/"
    )
    return pd.read_parquet(Path(f"gcs_data/{gcs_path}"))


@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtc-de-course-gcp-cred")
    df.to_gbq(
        destination_table=f"homework_2.{color}_trip",
        project_id="dtc-de-course-375108",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, month: int):
    """Main ETL Flow to load data into BigQuery"""

    df = extract_from_gcs(color, year, month)
    print(f"rows: {len(df)}")
    write_bq(df, color)


@flow()
def etl_gcs_to_bq_parent(color: str = "yellow", year: int = 2021, months: List[int] = [1, 2]) -> None:
    """Parent ETL Flow for etl_gcs_to_bq"""

    for month in months:
        etl_gcs_to_bq(color, year, month)


if __name__ == "__main__":
    q3_deployment = Deployment.build_from_flow(
        flow=etl_gcs_to_bq_parent,
        name="question-3-flow"
    )
    q3_deployment.apply()