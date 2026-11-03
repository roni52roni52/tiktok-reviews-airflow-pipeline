# TikTok Reviews Data Pipeline

End-to-end data engineering pipeline for processing TikTok Google Play reviews with Apache Airflow and loading the cleaned data into MongoDB.

## Architecture

```text
CSV input
   ↓
Apache Airflow: process_tiktok_reviews
   ├── wait for input file
   ├── validate input
   ├── normalize columns
   ├── replace missing values
   ├── sort by creation date
   └── clean review content
   ↓
Airflow Dataset
   ↓
Apache Airflow: load_reviews_to_mongodb
   ├── load data in batches
   ├── upsert documents by reviewId
   └── validate document count
   ↓
MongoDB
```

## Technologies

- Python 3.12
- Apache Airflow 2.11
- Pandas
- MongoDB 8
- PostgreSQL 16
- Docker Compose
- PyMongo

## Pipeline

### 1. Processing DAG

`process_tiktok_reviews`:

- waits for the source CSV file,
- handles an empty input file,
- validates required columns,
- renames `score` to `rating`,
- renames `at` to `created_date`,
- replaces empty and null values,
- sorts records by creation date,
- removes unwanted characters from review content,
- publishes an Airflow Dataset.

### 2. MongoDB loading DAG

`load_reviews_to_mongodb`:

- is automatically triggered by the processed Dataset,
- reads the CSV in batches of 5,000 rows,
- loads documents using idempotent upserts,
- creates a unique index on `reviewId`,
- validates the number of MongoDB documents.

## Data quality checks

The validation process checks:

- required columns,
- empty cells,
- literal `null` values,
- invalid dates,
- date ordering,
- ratings outside the 1–5 range,
- unwanted characters in review content.

Validated result:

```text
Rows: 307,057
Columns: 10
Empty cells: 0
Literal "null" values: 0
Invalid dates: 0
Dates sorted: True
Invalid ratings: 0
Rows with unwanted characters: 0
```

## Running the project

Place the source file at:

```text
data/input/tiktok_google_play_reviews.csv
```

Build and start the services:

```bash
docker compose build
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
```

Default local credentials:

```text
Username: admin
Password: admin
```

Enable and trigger:

```text
process_tiktok_reviews
```

The second DAG is triggered automatically after the processed Dataset is published.

## Validation

Run the processed-data validation:

```bash
docker compose exec airflow-scheduler \
  python /opt/airflow/scripts/validate_processed_data.py
```

Check the MongoDB document count:

```bash
docker compose exec mongodb mongosh --quiet --eval \
  "db.getSiblingDB('comments_db').tiktok_reviews.countDocuments({})"
```

Expected result:

```text
307057
```

## Idempotency

Documents are written using `reviewId` as the unique key. Re-running the loading DAG updates existing documents instead of creating duplicates.

## Project structure

```text
.
├── dags/
│   ├── process_reviews_dag.py
│   └── load_reviews_to_mongodb_dag.py
├── scripts/
│   └── validate_processed_data.py
├── data/
│   ├── input/
│   ├── staging/
│   └── processed/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md
```