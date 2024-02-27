import logging

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from app.clean import clean_client_data, clean_financial_data, join_dataframes


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


def test_clean_client_data_with_countries_argument(spark_session):
    # Arrange
    countries = ["United Kingdom", "Netherlands"]
    client_df = spark_session.createDataFrame([
        (1, "Alice", "Doe", "alicedoe@reuters.com", "United States"),
        (2, "Bob", "Johnson", "bobjohnson@email.com", "Netherlands"),
        (3, "Sam", "Smith", "samsmithn@email.com", "United Kingdom"),
    ], ["id", "first_name", "last_name", "email", "country"])

    expected_output_df = spark_session.createDataFrame([
        (2, "bobjohnson@email.com"),
        (3, "samsmithn@email.com"),
    ], ["id", "email"])

    # Act
    cleaned_df = clean_client_data(client_df, countries)

    # Assert
    assert_df_equality(expected_output_df, cleaned_df)


def test_clean_financial_data(spark_session):
    # Arrange
    countries = ["United Kingdom", "Netherlands"]
    financial_df = spark_session.createDataFrame([
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron", 4175006996999270),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb", 3587679584356527),
        (3, "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute", 201876885481838)
    ], ["id", "btc_a", "cc_t", "cc_n"])

    expected_output_df = spark_session.createDataFrame([
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron"),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
        (3, "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute")
    ], ["id", "btc_a", "cc_t"])

    # Act
    cleaned_df = clean_financial_data(financial_df)

    # Assert
    assert_df_equality(expected_output_df, cleaned_df)


def test_clean_join_data(spark_session):
    # Arrange
    client_df = spark_session.createDataFrame([
        (2, "bobjohnson@email.com"),
        (3, "samsmithn@email.com"),
    ], ["id", "email"])

    financial_df = spark_session.createDataFrame([
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa - electron"),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
        (3, "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute")
    ], ["id", "btc_a", "cc_t"])

    expected_output_df = spark_session.createDataFrame([
        (2, "bobjohnson@email.com", "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
        (3, "samsmithn@email.com", "1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd", "diners - club - enroute")
    ], ["id", "email", "btc_a", "cc_t"])

    # Act
    cleaned_df = join_dataframes(client_df, financial_df)

    # Assert
    assert_df_equality(expected_output_df, cleaned_df, ignore_row_order=True)
