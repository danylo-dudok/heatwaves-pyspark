import pytest
from datetime import date

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from src.common import (
    clean_data,
    filter_needed_data,
    aggregate_daily,
    with_date,
    with_prev_date,
    with_date_diff,
    with_group
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_clean_data(spark):
    data = [
        (None, None),
        (10.0, None),
        (None, 5.0),
        (10.0, 5.0),
    ]
    schema = StructType([
        StructField("TX_DRYB_10", FloatType(), True),
        StructField("TN_10CM_PAST_6H_10", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    result_df = clean_data(df)

    assert result_df.count() == 3


def test_filter_needed_data(spark):
    data = [
        ("De Bilt", "2003-07-15", 10.0, 5.0),
        ("Amsterdam", "2003-07-15", 12.0, 6.0),
        ("De Bilt", "2002-12-31", 8.0, 2.0),
    ]
    schema = StructType([
        StructField("NAME", StringType(), True),
        StructField("DTG", StringType(), True),
        StructField("TX_DRYB_10", FloatType(), True),
        StructField("TN_10CM_PAST_6H_10", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    filtered_df = filter_needed_data(df, ["De Bilt"], "2003-01-01")
    rows = filtered_df.select("NAME").collect()

    assert len(rows) == 1
    assert rows[0]["NAME"] == "De Bilt"


def test_with_date(spark):
    data = [("2003-07-15",)]
    schema = StructType([StructField("DTG", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    df = with_date(df)
    row = df.first()

    assert isinstance(row["DATE"], date)
    assert row["DATE"].isoformat() == "2003-07-15"


def test_aggregate_daily(spark):
    data = [
        ("2003-07-15", 10.0, 5.0),
        ("2003-07-15", 15.0, 3.0),
        ("2003-07-16", 20.0, 12.0)
    ]
    schema = StructType([
        StructField("DATE", StringType(), True),
        StructField("TX_DRYB_10", FloatType(), True),
        StructField("TN_10CM_PAST_6H_10", FloatType(), True),
    ])
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd"))
    aggregated = aggregate_daily(df)

    rows = {row["DATE"].isoformat(): (row["MAX_DAILY_T"], row["MIN_TEMP"]) for row in aggregated.collect()}
    assert rows["2003-07-15"] == (15.0, 3.0)
    assert rows["2003-07-16"] == (20.0, 12.0)


def test_with_prev_date(spark):
    data = [("2003-07-15",), ("2003-07-16",)]
    schema = StructType([StructField("DATE", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd"))
    df = with_prev_date(df)
    rows = df.orderBy("DATE").collect()

    assert rows[0]["PREV_DATE"] is None
    assert rows[1]["PREV_DATE"].isoformat() == "2003-07-15"


def test_with_date_diff(spark):
    data = [("2003-07-15",), ("2003-07-16",)]
    schema = StructType([StructField("DATE", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd"))
    df = with_prev_date(df)
    df = with_date_diff(df)
    rows = df.orderBy("DATE").collect()

    assert rows[0]["DAY_DIFF"] is None
    assert rows[1]["DAY_DIFF"] == 1


def test_with_group(spark):
    data = [("2003-07-15",), ("2003-07-16",), ("2003-07-18",)]
    schema = StructType([StructField("DATE", StringType(), True)])
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("DATE", F.to_date(F.col("DATE"), "yyyy-MM-dd"))
    df = with_prev_date(df)
    df = with_date_diff(df)
    df = with_group(df)
    rows = df.orderBy("DATE").collect()

    assert rows[0]["GROUP_ID"] == 1
    assert rows[1]["GROUP_ID"] == 1
    assert rows[2]["GROUP_ID"] == 2
