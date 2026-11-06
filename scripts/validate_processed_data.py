import pandas as pd


PROCESSED_PATH = (
    "/opt/airflow/data/processed/"
    "tiktok_reviews_processed.csv"
)

REQUIRED_COLUMNS = {
    "reviewId",
    "content",
    "rating",
    "created_date",
}

ALLOWED_RATING_MIN = 1
ALLOWED_RATING_MAX = 5


def load_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        keep_default_na=False,
    )


def validate_columns(dataframe: pd.DataFrame) -> None:
    missing_columns = (
        REQUIRED_COLUMNS - set(dataframe.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing columns: {sorted(missing_columns)}"
        )


def count_empty_cells(dataframe: pd.DataFrame) -> int:
    return int(
        dataframe.astype(str)
        .apply(
            lambda column: column
            .str.strip()
            .eq("")
        )
        .sum()
        .sum()
    )


def count_literal_nulls(
    dataframe: pd.DataFrame,
) -> int:
    return int(
        dataframe.astype(str)
        .apply(
            lambda column: column
            .str.strip()
            .str.lower()
            .eq("null")
        )
        .sum()
        .sum()
    )


def validate_dates(
    dataframe: pd.DataFrame,
) -> tuple[int, bool]:
    created_dates = pd.to_datetime(
        dataframe["created_date"],
        errors="coerce",
    )

    invalid_dates = int(
        created_dates.isna().sum()
    )
    dates_sorted = bool(
        created_dates.is_monotonic_increasing
    )

    return invalid_dates, dates_sorted


def count_invalid_ratings(
    dataframe: pd.DataFrame,
) -> int:
    ratings = pd.to_numeric(
        dataframe["rating"],
        errors="coerce",
    )

    invalid_mask = (
        ratings.isna()
        | ~ratings.between(
            ALLOWED_RATING_MIN,
            ALLOWED_RATING_MAX,
        )
    )

    return int(invalid_mask.sum())


def count_unwanted_content(
    dataframe: pd.DataFrame,
) -> int:
    unwanted_mask = (
        dataframe["content"]
        .astype(str)
        .str.contains(
            r"""[^\w\s.,!?;:'"()\-]""",
            regex=True,
            na=False,
        )
    )

    return int(unwanted_mask.sum())


def count_duplicate_review_ids(
    dataframe: pd.DataFrame,
) -> int:
    return int(
        dataframe["reviewId"]
        .duplicated(keep=False)
        .sum()
    )


def raise_if_invalid(
    dataframe: pd.DataFrame,
    empty_cells: int,
    literal_nulls: int,
    invalid_dates: int,
    dates_sorted: bool,
    invalid_ratings: int,
    unwanted_content: int,
    duplicate_review_ids: int,
) -> None:
    errors: list[str] = []

    if dataframe.empty:
        errors.append("The processed dataset is empty.")

    if empty_cells > 0:
        errors.append(
            f"Empty cells: {empty_cells}"
        )

    if literal_nulls > 0:
        errors.append(
            f'Literal "null" values: {literal_nulls}'
        )

    if invalid_dates > 0:
        errors.append(
            f"Invalid dates: {invalid_dates}"
        )

    if not dates_sorted:
        errors.append(
            "Dates are not sorted in ascending order."
        )

    if invalid_ratings > 0:
        errors.append(
            f"Invalid ratings: {invalid_ratings}"
        )

    if unwanted_content > 0:
        errors.append(
            "Rows with unwanted characters: "
            f"{unwanted_content}"
        )

    if duplicate_review_ids > 0:
        errors.append(
            "Rows with duplicated reviewId: "
            f"{duplicate_review_ids}"
        )

    if errors:
        formatted_errors = "\n- ".join(errors)

        raise ValueError(
            "Data validation failed:\n"
            f"- {formatted_errors}"
        )


def main() -> None:
    dataframe = load_data(PROCESSED_PATH)

    validate_columns(dataframe)

    empty_cells = count_empty_cells(dataframe)
    literal_nulls = count_literal_nulls(dataframe)
    invalid_dates, dates_sorted = validate_dates(
        dataframe
    )
    invalid_ratings = count_invalid_ratings(
        dataframe
    )
    unwanted_content = count_unwanted_content(
        dataframe
    )
    duplicate_review_ids = (
        count_duplicate_review_ids(dataframe)
    )

    print(f"Rows: {len(dataframe):,}")
    print(f"Columns: {len(dataframe.columns)}")
    print(f"Empty cells: {empty_cells}")
    print(
        f'Literal "null" values: {literal_nulls}'
    )
    print(f"Invalid dates: {invalid_dates}")
    print(f"Dates sorted: {dates_sorted}")
    print(f"Invalid ratings: {invalid_ratings}")
    print(
        "Rows with unwanted characters: "
        f"{unwanted_content}"
    )
    print(
        "Rows with duplicated reviewId: "
        f"{duplicate_review_ids}"
    )

    raise_if_invalid(
        dataframe=dataframe,
        empty_cells=empty_cells,
        literal_nulls=literal_nulls,
        invalid_dates=invalid_dates,
        dates_sorted=dates_sorted,
        invalid_ratings=invalid_ratings,
        unwanted_content=unwanted_content,
        duplicate_review_ids=duplicate_review_ids,
    )

    print("Processed data validation passed.")


if __name__ == "__main__":
    main()