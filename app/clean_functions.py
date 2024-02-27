"""Module providing functions for data cleaning"""
from typing import Dict, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read data into a DataFrame.
    :param spark: SparkSession which
    :param file_path: string path to the file
    :return: DataFrame
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)


def filter_data(dataframe: DataFrame, column: str, allowed_values: Iterable = None) -> DataFrame:
    """
    Filter data by a column.
    :param dataframe: DataFrame
    :param column: string column name
    :param allowed_values: list of allowed values
    :return: DataFrame
    """
    return dataframe.filter(col(column).isin(allowed_values))


def rename_columns(dataframe: DataFrame, name_mapping: Dict[str, str]) -> DataFrame:
    """
    Rename columns in a DataFrame.
    :param dataframe: DataFrame
    :param name_mapping: dictionary mapping old column names to new column names
    :return: DataFrame
    """
    return dataframe.withColumnsRenamed(name_mapping)
