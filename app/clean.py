"""Module containing the cleaning workflow and the specific transformation functions"""
import inspect
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

import app.clean_functions as cf
from app.logger_config import get_custom_logger

spark = (SparkSession.builder
         .master("local")
         .appName("DataCleaning")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

logger = get_custom_logger(__name__)


def clean_client_data(client_df: DataFrame, countries: List[str] = None) -> DataFrame:
    """
    The function cleans the client data.
    :param client_df: Dataframe containing the client data
    :param countries: List of countries for filtering
    :return: Dataframe
    """
    if countries:
        logger.debug(
            "Countries used as filter arguments: %s in %s function",
            countries, inspect.currentframe().f_code.co_name
        )
        client_df = cf.filter_data(client_df, "country", countries)
    return client_df.drop("first_name", "last_name", "country")


def clean_financial_data(financial_df: DataFrame) -> DataFrame:
    """
    The function cleans the financial data.
    :param financial_df: Dataframe containing the financial data
    :return: Dataframe
    """
    return financial_df.drop("cc_n")


def join_dataframes(client_df: DataFrame, financial_df: DataFrame) -> DataFrame:
    """
    The function joins the two dataframes on the id column.
    :param client_df: Dataframe containing the client data
    :param financial_df: Dataframe containing the financial data
    :return: Dataframe
    """
    return client_df.join(financial_df, on="id", how="left").drop("client_id")


def log_dataframe_metadata(dataframe: DataFrame, dataframe_name: str, step_name: str) -> None:
    """
    The function logs the shape and columns of a dataframe after a specific step.
    :param dataframe: Dataframe for which statistics will be logged
    :param dataframe_name: Name of the dataframe in the log
    :param step_name: Process step that will be logged
    """
    logger.info("%s shape after %s: (%d, %d)", dataframe_name, step_name, dataframe.count(),
                len(dataframe.columns))
    logger.debug("%s columns after loading: %s", dataframe_name, dataframe.columns)


def process_data(client_path: str, financial_path: str, countries: List[str]) -> DataFrame:
    """
    The function processes the datasets and saves the results in client_data directory.
    :param client_path: filepath to client data
    :param financial_path: filepath to financial data
    :param countries: list of countries used for filtering
    :return:
    """
    # Read data into a DataFrame
    client_df = cf.read_data(spark, client_path)
    financial_df = cf.read_data(spark, financial_path)
    log_dataframe_metadata(client_df, "Client Dataset", "Extraction")
    log_dataframe_metadata(financial_df, "Financial Dataset", "Extraction")

    # Clean and prepare for joining data
    client_df = clean_client_data(client_df, countries)
    financial_df = clean_financial_data(financial_df)
    log_dataframe_metadata(client_df, "Client Dataset", "Cleaning")
    log_dataframe_metadata(financial_df, "Financial Dataset", "Cleaning")

    # Join the two DataFrames
    processed_df = join_dataframes(client_df, financial_df)
    log_dataframe_metadata(processed_df, "Processed Dataset", "Joining")

    # Rename Columns
    processed_df = cf.rename_columns(processed_df,
                                     name_mapping={"id": "client_identifier",
                                                   "btc_a": "bitcoin_address",
                                                   "cc_t": "credit_card_type"})
    log_dataframe_metadata(processed_df, "Processed Dataset", "Renaming")
    logger.info("\n-----------------------------------------------------------------------------\n")
    return processed_df
