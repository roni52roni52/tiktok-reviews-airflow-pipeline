from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup


INPUT_PATH = (
    "/opt/airflow/data/input/tiktok_google_play_reviews.csv"
)
NORMALIZED_PATH = (
    "/opt/airflow/data/staging/01_normalized.csv"
)
NULLS_REPLACED_PATH = (
    "/opt/airflow/data/staging/02_nulls_replaced.csv"
)
SORTED_PATH = (
    "/opt/airflow/data/staging/03_sorted.csv"
)
PROCESSED_PATH = (
    "/opt/airflow/data/processed/tiktok_reviews_processed.csv"
)

PROCESSED_DATASET = Dataset(
    "file:///opt/airflow/data/processed/"
    "tiktok_reviews_processed.csv"
)


@dag(
    dag_id="process_tiktok_reviews",
    description="Process TikTok reviews and publish a dataset",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["tiktok", "pandas", "dataset"],
)
def process_tiktok_reviews() -> None:
    wait_for_input_file = FileSensor(
        task_id="wait_for_input_file",
        filepath=INPUT_PATH,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    @task.branch(task_id="check_if_file_has_data")
    def check_if_file_has_data() -> str:
        try:
            sample = pd.read_csv(
                INPUT_PATH,
                nrows=1,
                keep_default_na=False,
            )
        except pd.errors.EmptyDataError:
            return "log_empty_file"

        if sample.empty:
            return "log_empty_file"

        return "processing.normalize_columns"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=(
            'echo "Input file is empty. '
            'No records will be processed."'
        ),
    )

    with TaskGroup(group_id="processing") as processing:

        @task(task_id="normalize_columns")
        def normalize_columns() -> str:
            dataframe = pd.read_csv(
                INPUT_PATH,
                keep_default_na=False,
            )

            required_columns = {
                "content",
                "score",
                "at",
            }
            missing_columns = (
                required_columns - set(dataframe.columns)
            )

            if missing_columns:
                raise ValueError(
                    "Missing required columns: "
                    f"{sorted(missing_columns)}"
                )

            dataframe = dataframe.rename(
                columns={
                    "score": "rating",
                    "at": "created_date",
                }
            )

            Path(NORMALIZED_PATH).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            dataframe.to_csv(
                NORMALIZED_PATH,
                index=False,
            )

            return NORMALIZED_PATH

        @task(task_id="replace_nulls")
        def replace_nulls(input_path: str) -> str:
            dataframe = pd.read_csv(
                input_path,
                keep_default_na=False,
            )

            dataframe = dataframe.replace(
                to_replace=r"(?i)^\s*(?:null)?\s*$",
                value="-",
                regex=True,
            )

            dataframe.to_csv(
                NULLS_REPLACED_PATH,
                index=False,
            )

            return NULLS_REPLACED_PATH

        @task(task_id="sort_by_created_date")
        def sort_by_created_date(
            input_path: str,
        ) -> str:
            dataframe = pd.read_csv(
                input_path,
                keep_default_na=False,
            )

            dataframe["created_date"] = pd.to_datetime(
                dataframe["created_date"],
                errors="raise",
            )

            dataframe = dataframe.sort_values(
                by="created_date",
                ascending=True,
            )

            dataframe.to_csv(
                SORTED_PATH,
                index=False,
            )

            return SORTED_PATH

        @task(
            task_id="clean_content",
            outlets=[PROCESSED_DATASET],
        )
        def clean_content(input_path: str) -> str:
            dataframe = pd.read_csv(
                input_path,
                keep_default_na=False,
            )

            cleaned_content = (
                dataframe["content"]
                .astype(str)
                .str.replace(
                    r"""[^\w\s.,!?;:'"()\-]""",
                    "",
                    regex=True,
                )
                .str.replace(
                    r"\s+",
                    " ",
                    regex=True,
                )
                .str.strip()
            )

            dataframe["content"] = cleaned_content.replace(
                to_replace=r"(?i)^\s*(?:null)?\s*$",
                value="-",
                regex=True,
            )

            Path(PROCESSED_PATH).parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            dataframe.to_csv(
                PROCESSED_PATH,
                index=False,
            )

            return PROCESSED_PATH

        normalized_file = normalize_columns()
        nulls_replaced_file = replace_nulls(
            normalized_file
        )
        sorted_file = sort_by_created_date(
            nulls_replaced_file
        )
        clean_content(sorted_file)

    branch = check_if_file_has_data()

    wait_for_input_file >> branch
    branch >> log_empty_file
    branch >> processing


process_tiktok_reviews()