from datetime import timedelta
from airflow.decorators import dag
from airflow.models import Variable

import dlt
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup

import os

# modify the default task arguments - all the tasks created for dlt pipeline will inherit it
# - set e-mail notifications
# - we set retries to 0 and recommend to use `PipelineTasksGroup` retry policies with tenacity library, you can also retry just extract and load steps
# - execution_timeout is set to 20 hours, tasks running longer that that will be terminated

default_task_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'jsgane7@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(hours=20),
}

# modify the default DAG arguments
# - the schedule below sets the pipeline to `@daily` be run each day after midnight, you can use crontab expression instead
# - start_date - a date from which to generate backfill runs
# - catchup is False which means that the daily runs from `start_date` will not be run, set to True to enable backfill
# - max_active_runs - how many dag runs to perform in parallel. you should always start with 1


@dag(
    dag_id="github_dag", # define dag id
    schedule_interval='@daily',
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args
)
def load_data():
    # Store snowflake credential in env variable
    os.environ["DESTINATION__CREDENTIALS"] = Variable.get("snowflake_credentials") ### var for snowflake

    # set `use_data_folder` to True to store temporary data on the `data` bucket. Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup("pipeline_dlt_api_snowflake", use_data_folder=False, wipe_local_data=True)

    # import your source from pipeline script
    from api_source import github_source

    start_date = Variable.get("start_date", "")
    end_date = Variable.get("end_date", "")

    if start_date and end_date:
        github_source.forks.apply_hints(
            incremental = dlt.sources.incremental(
                "created_at",
                initial_value = start_date #"2025-12-01T00:00:00Z",
                end_value = end_date #"2026-01-01T00:00:00Z",
                row_order = "asc"
            )
        )
    # modify the pipeline parameters 
    pipeline = dlt.pipeline(pipeline_name='github_pipeline',
                     dataset_name='github_data_airflow',
                     destination='snowflake',
                     full_refresh=False # must be false if we decompose
                     )
    # create the source, the "serialize" decompose option will converts dlt resources into Airflow tasks. use "none" to disable it
    # To use parallelzation set decompose to parallel-isolated
    tasks.add_run(pipeline, github_source, decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)


load_data()