from typing import Dict, Iterable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def read_data(spark, file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)


def filter_data(df: DataFrame, column: str, allowed_values: Iterable = None) -> DataFrame:
    return df.filter(col(column).isin(allowed_values))


def rename_columns(df: DataFrame, name_mapping: Dict[str, str]) -> DataFrame:
    return df.withColumnsRenamed(name_mapping)
