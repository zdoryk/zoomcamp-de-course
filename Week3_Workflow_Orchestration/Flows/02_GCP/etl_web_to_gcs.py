# Take the data from the Web
# Do a bit of cleaning
# Save as Parquet
# And Push to GCS

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def fetch(url: str) -> pd.DataFrame:
    """
    Read taxi data from web into Pandas DataFrame
    """
    df = pd.read_csv(url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtypes
    """
    # df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    # df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(retries=3, log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write a dataframe locally as a parquet file
    """
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path=path, compression="gzip")
    return path


@task(retries=3, log_prints=True)
def write_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name='Main_ETL', log_prints=True)
def etl_web_to_gcs() -> None:
    """
    The main ETL func
    """
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{str(month).zfill(2)}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_cleaned = clean(df)
    path = write_local(df_cleaned, color, dataset_file)
    write_gcs(path)
    # print(df_cleaned)


if __name__ == "__main__":
    etl_web_to_gcs()

#%%
