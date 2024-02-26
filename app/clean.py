import logging
import sys
import inspect
from typing import List

import coloredlogs
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from logger_config import get_custom_logger

spark = (SparkSession.builder
         .master("local")
         .appName("DataCleaning")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

logger = get_custom_logger(__name__)
coloredlogs.install(level=logging.INFO, logger=logger, isatty=True,
                    fmt="%(asctime)s %(levelname)-8s %(message)s",
                    stream=sys.stdout,
                    datefmt='%Y-%m-%d %H:%M:%S')


def read_data(file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)


def clean_client_data(client_df: DataFrame, countries: List[str] = None) -> DataFrame:
    logger.debug(f"Countries used as filter arguments: {countries} in {inspect.currentframe().f_code.co_name} function")
    if countries:
        client_df = client_df.filter(client_df.country.isin(countries))
    return (
        client_df
        .drop("first_name", "last_name", "country")
        # .dropDuplicates()
    )


def clean_financial_data(financial_df: DataFrame) -> DataFrame:
    return (
        financial_df
        .drop("cc_n")
    )


def join_dataframes(client_df: DataFrame, financial_df: DataFrame) -> DataFrame:
    return (
        client_df
        .join(financial_df, on="id", how="inner")
        .drop("client_id")
        .withColumnsRenamed({"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"})
    )


def process_data(client_path: str, financial_path: str, countries: List[str]) -> None:
    # Read data into a DataFrame
    client_df = read_data(client_path)
    financial_df = read_data(financial_path)
    logger.info(f"Client dataset shape after loading: ({client_df.count()}, {len(client_df.columns)})")
    logger.debug(f"Client dataset columns after loading: {client_df.columns}")
    logger.info(f"Financial dataset shape after loading: ({financial_df.count()}, {len(financial_df.columns)})")
    logger.debug(f"Financial dataset columns after loading: {financial_df.columns}")

    # Clean and prepare for joining data
    client_df = clean_client_data(client_df, countries)
    financial_df = clean_financial_data(financial_df)
    logger.info(f"Client dataset shape after cleaning: ({client_df.count()}, {len(client_df.columns)})")
    logger.debug(f"Client dataset columns after cleaning: {client_df.columns}")
    logger.info(f"Financial dataset shape after cleaning: ({financial_df.count()}, {len(financial_df.columns)})")
    logger.debug(f"Financial dataset columns after cleaning: {financial_df.columns}")

    # Join the two DataFrames
    processed_df = join_dataframes(client_df, financial_df)
    logger.info(f"Shape after joining: ({processed_df.count()}, {len(processed_df.columns)})")
    logger.debug(f"Columns after joining: {processed_df.columns}")

    processed_df.write.csv("client_data/client_data.csv", mode="overwrite", header=True)
