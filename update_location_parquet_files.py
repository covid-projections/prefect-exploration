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

from prefect import flatten, Flow, task, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.tasks.shell import ShellTask
from prefect.tasks.secrets import EnvVarSecret

DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
DATA_PATH.mkdir(parents=True, exist_ok=True)
FILENAME_PREFIX = "can_scrape_api_covid_us"


@task
def fetch_location_ids(connstr: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        result = conn.execute(
            sa.text("SELECT DISTINCT location_id FROM public.covid_us;")
        )
        location_ids = [row[0] for row in result.fetchall()]
        return location_ids


@task
def create_location_parquet(connstr: str, location_id: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        query = sa.text(
            "SELECT * FROM public.covid_us WHERE location_id = :location_id;"
        ).bindparams(location_id=location_id)

        # Read rows from PostgreSQL into Pandas dataframe.
        # TODO: Can we read to plain dicts/tuples instead and remove Pandas as
        # a dependency? Would probably use pyarrow instead for Parquet IO.
        df = pd.read_sql_query(query, conn)

    # Write vintage file.
    ts = prefect.context.scheduled_start_time
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
    vintage_fn = f"{FILENAME_PREFIX}_{location_id}_{dt_str}.parquet"
    df.to_parquet(DATA_PATH / vintage_fn, index=False)

    # Replace primary file.
    fn = f"{FILENAME_PREFIX}_{location_id}.parquet"
    df.to_parquet(DATA_PATH / fn, index=False)

    return vintage_fn, fn


@task
def get_gcs_cmd(fn):
    return f"gsutil acl ch -u AllUsers:R gs://can-scrape-outputs/final/{fn}"


def main():
    with Flow("update-location-parquet-files", executor=LocalDaskExecutor()) as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        location_ids = fetch_location_ids(connstr)
        filename_tuples = create_location_parquet.map(
            connstr=unmapped(connstr), location_id=location_ids
        )

        file_permission_commands = get_gcs_cmd.map(flatten(filename_tuples))
        # TODO: figure out how to test this
        # ShellTask().map(file_permission_commands)

    flow.register(project_name="prefect-exploration")


if __name__ == "__main__":
    main()
