import logging

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from app.clean_functions import filter_data, rename_columns, read_data


@pytest.fixture(autouse=True)
def disable_logging():
    logging.disable(logging.CRITICAL)  # Disable all logging calls of CRITICAL and below
    yield
    logging.disable(logging.NOTSET)  # Re-enable logging


@pytest.fixture(scope="session")
def spark_session():
    """
    PyTest fixture for creating a SparkSession.
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-spark-test-session") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_read_data(spark_session):
    # Arrange
    file_path = "tests/example1.csv"
    expected_columns = ['email', 'first_name', 'country', 'last_name', 'id']

    # Act
    df = read_data(spark_session, file_path)

    # Assert
    assert set(df.columns) == set(expected_columns)


def test_filter_data(spark_session):
    # Arrange
    input_df = spark_session.createDataFrame([
        ("1", "United Kingdom"),
        ("2", "United States"),
        ("3", "Netherlands"),
    ], ["id", "country"])

    expected_output_df = spark_session.createDataFrame([
        ("1", "United Kingdom"),
        ("3", "Netherlands"),
    ], ["id", "country"])

    # Act
    output_df = filter_data(input_df,
                            "country",
                            ["Netherlands", "United Kingdom", ])

    # Assert
    assert_df_equality(output_df, expected_output_df)


def test_rename_columns(spark_session):
    # Arrange
    input_df = spark_session.createDataFrame([
        (1, "a", "x"),
        (2, "b", "y"),
        (3, "c", "z")
    ], ["id", "old_col_1", "old_col_2"])

    name_mapping = {
        "old_col_1": "new_col_1",
        "old_col_2": "new_col_2"
    }

    expected_df = spark_session.createDataFrame([
        (1, "a", "x"),
        (2, "b", "y"),
        (3, "c", "z")
    ], ["id", "new_col_1", "new_col_2"])

    # Act
    output_df = rename_columns(input_df, name_mapping)

    # Assert
    assert_df_equality(output_df, expected_df)
