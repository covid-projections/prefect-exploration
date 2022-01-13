# This is similar to update_api_view.py (which defines the flow named
# UpdateParquetFiles) but instead of producing a single monolithic Parquet
# file, it produces one Parquet file for each `location_id` in the PostgreSQL
# database's `covid_us` view.

import os
import pandas as pd
import pathlib
import prefect
import pyarrow
import sqlalchemy as sa

from google.cloud import storage
from prefect import flatten, Flow, task, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run
from prefect.tasks.secrets import EnvVarSecret
from typing import List

GEO_DATA_PATH = "https://media.githubusercontent.com/media/covid-projections/covid-data-model/main/data/geo-data.csv"


@task
def location_ids_for(state: str, geo_data_path: str = GEO_DATA_PATH) -> List[str]:
    df = pd.read_csv(geo_data_path)
    df = df[df["state"] == state]
    return df["location_id"].tolist()


@task
def fetch_location_ids(connstr: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("SELECT DISTINCT location_id FROM public.covid_us;")
        )
        location_ids = [row[0] for row in result.fetchall()]
        return location_ids


def upload(local_path, bucket, blob_path):
    logger = prefect.context.get("logger")

    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

    logger.info(f"Uploaded to {bucket.name}/{blob_path}")


@task(task_run_name="create_location_parquet ({location_id})")
def create_location_parquet(connstr: str, location_id: str):
    logger = prefect.context.get("logger")

    logger.info(location_id)

    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        query = sa.text(
            "SELECT * FROM public.covid_us WHERE location_id = :location_id;"
        ).bindparams(location_id=location_id)

        # Read rows from PostgreSQL into Pandas dataframe.
        # TODO: Can we read to plain dicts/tuples instead and remove Pandas as
        # a dependency? Would probably use pyarrow instead for Parquet IO.
        df = pd.read_sql_query(query, conn)

    data_dir = pathlib.Path("/home/clizzin/prefect-exploration/can-scrape-outputs")
    data_dir.mkdir(parents=True, exist_ok=True)
    filename_prefix = "can_scrape_api_covid_us"

    # Write vintage file.
    ts = prefect.context.scheduled_start_time
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
    vintage_fn = f"{filename_prefix}_{location_id}_{dt_str}.parquet"
    vintage_path = data_dir / vintage_fn
    df.to_parquet(vintage_path, index=False)
    logger.info(vintage_fn)

    # Replace primary file.
    fn = f"{filename_prefix}_{location_id}.parquet"
    path = data_dir / fn
    df.to_parquet(path, index=False)
    logger.info(fn)

    # Upload to Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket("prefect-exploration")
    upload(path, bucket, f"location-parquet-files/{fn}")
    upload(vintage_path, bucket, f"location-parquet-files/{vintage_fn}")

    return vintage_fn, fn


def create_flow(state):
    with Flow(
        f"update-location-parquet-files ({state})", storage=GCS(bucket="prefect-flows")
    ) as flow:

        location_ids = location_ids_for(state, GEO_DATA_PATH)
        filename_tuples = create_location_parquet.map(
            connstr=unmapped(connstr), location_id=location_ids
        )
    flow.run_config = UniversalRun(labels=["dev"])
    return flow


def main():
    df = pd.read_csv(GEO_DATA_PATH)
    states = df["state"].dropna().unique().tolist()

    # Create one flow for each state.

    flow_ids = []
    for state in states:
        flow = create_flow(state)
        flow_id = flow.register(project_name="prefect-exploration")
        flow_ids.append(flow_id)

    # Register a parent flow that runs every state's flow.

    with Flow(
        "update-location-parquet-files",
        storage=GCS(bucket="prefect-flows")
    ) as parent_flow:

        for flow_id in flow_ids:
            create_flow_run(flow_id)

    parent_flow.run_config = UniversalRun(labels=["dev"])
    parent_flow.register(project_name="prefect-exploration")


if __name__ == "__main__":
    main()
