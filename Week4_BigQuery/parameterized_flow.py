# Take the data from the Web
# Do a bit of cleaning
# Save as Parquet
# And Push to GCS
import os
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, log_prints=True)
def write_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name='Main_ETL', log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    ETL sub flow
    """
    dataset_file = f"{color}_tripdata_{year}-{str(month).zfill(2)}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"
    os.system(f"wget {dataset_url} -O data/{color}/{dataset_file}")

    write_gcs(f'data/fhv/{dataset_file}.csv.gz')


@flow()
def etl_parent_flow(
        months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "fhv"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)
