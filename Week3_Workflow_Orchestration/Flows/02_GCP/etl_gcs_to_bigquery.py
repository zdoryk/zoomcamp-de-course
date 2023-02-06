# Take the data from the GCS
# And put the data to a BigQuery

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: str, month: str) -> Path:
    """
    Download data from GCS
    """

    dataset_file = f"{color}_tripdata_{year}-{str(month).zfill(2)}"
    path = Path(f"data/{color}/{dataset_file}.parquet")
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=path, local_path='./data_from_bq/')
    # df.to_parquet(path=path, compression="gzip")
    return Path(f'./data_from_bq/{path}')


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """
    Data cleaning example
    """
    df = pd.read_parquet(path)
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def load_to_bq(df: pd.DataFrame) -> None:
    """
    Load DataFrame to BigQuery
    """
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-course-376115",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow(name='ETL from GCS to BQ', log_prints=True)
def etl_gcs_to_bq():
    """
    Main ETL flow to load data into Big Query
    """
    color = "yellow"
    year = 2020
    month = 11

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    load_to_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()
