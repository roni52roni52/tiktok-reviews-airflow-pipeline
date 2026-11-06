from datetime import timedelta

import pandas as pd
import pendulum

from pymongo import ReplaceOne

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook


PROCESSED_PATH = (
    "/opt/airflow/data/processed/tiktok_reviews_processed.csv"
)

PROCESSED_DATASET = Dataset(
    "file:///opt/airflow/data/processed/"
    "tiktok_reviews_processed.csv"
)

DATABASE_NAME = "comments_db"
COLLECTION_NAME = "tiktok_reviews"
BATCH_SIZE = 5_000


@dag(
    dag_id="load_reviews_to_mongodb",
    description="Load processed TikTok reviews into MongoDB",
    schedule=[PROCESSED_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["tiktok", "mongodb", "dataset"],
)
def load_reviews_to_mongodb() -> None:

    @task(task_id="load_reviews")
    def load_reviews() -> dict[str, int]:
        mongo_hook = MongoHook(
            mongo_conn_id="mongo_default"
        )

        processed_rows = 0
        inserted_rows = 0
        updated_rows = 0

        with mongo_hook.get_conn() as client:
            database = client[DATABASE_NAME]
            collection = database[COLLECTION_NAME]

            collection.create_index(
                "reviewId",
                unique=True,
            )

            chunks = pd.read_csv(
                PROCESSED_PATH,
                keep_default_na=False,
                chunksize=BATCH_SIZE,
            )

            for dataframe in chunks:
                dataframe = dataframe.drop_duplicates(
                    subset=["reviewId"],
                    keep="last",
                )

                documents = dataframe.to_dict(
                    orient="records"
                )

                operations = [
                    ReplaceOne(
                        {
                            "reviewId": document["reviewId"]
                        },
                        document,
                        upsert=True,
                    )
                    for document in documents
                ]

                if not operations:
                    continue

                result = collection.bulk_write(
                    operations,
                    ordered=False,
                )

                processed_rows += len(documents)
                inserted_rows += result.upserted_count
                updated_rows += result.modified_count

        return {
            "processed_rows": processed_rows,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
        }

    @task(task_id="validate_mongodb")
    def validate_mongodb(
        load_result: dict[str, int],
    ) -> None:
        mongo_hook = MongoHook(
            mongo_conn_id="mongo_default"
        )

        with mongo_hook.get_conn() as client:
            collection = (
                client[DATABASE_NAME][COLLECTION_NAME]
            )

            mongodb_count = collection.count_documents({})

            expected_count = int(
                pd.read_csv(
                    PROCESSED_PATH,
                    usecols=["reviewId"],
                    keep_default_na=False,
                )["reviewId"].nunique()
            )

            if mongodb_count != expected_count:
                raise ValueError(
                    "MongoDB document count does not match "
                    f"the source: MongoDB={mongodb_count}, "
                    f"source={expected_count}"
                )

            print(f"Load result: {load_result}")
            print(f"MongoDB documents: {mongodb_count}")
            print("MongoDB validation passed.")

    load_result = load_reviews()
    validate_mongodb(load_result)


load_reviews_to_mongodb()