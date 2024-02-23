from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local")
         .appName("DataCleaning")
         .getOrCreate())

SELECTED_COUNTRIES = ["UK", "NL"]


def read_data(file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)


def clean_client_data(client_df: DataFrame) -> DataFrame:
    return (
        client_df
        .filter(client_df.country.isin(SELECTED_COUNTRIES))
        .drop("PII")
    )


def clean_financial_data(financial_df: DataFrame) -> DataFrame:
    return financial_df.drop("Creditcard")


def join_dataframes(client_df: DataFrame, financial_df) -> DataFrame:
    return (
        client_df
        .join(financial_df, on="client_id", how="inner")
        .drop("client_id")
        .withColumnsRenamed({"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"})
    )
