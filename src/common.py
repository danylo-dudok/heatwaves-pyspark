"""Common module to determine temperature waves."""

from pyspark.sql import DataFrame, Window, functions as F


date_window = Window.orderBy("DATE")


def clean_data(df: DataFrame) -> DataFrame:
    """
    Exclude rows with missing temperature values required for coldwave detection.
    Coldwave detection requires both the maximum (TX_DRYB_10) and minimum (TN_10CM_PAST_6H_10) temperatures.
    """
    return df.filter(F.col("TX_DRYB_10").isNotNull() | F.col("TN_10CM_PAST_6H_10").isNotNull())


def filter_needed_data(df: DataFrame, cities: list[str], start_date: str) -> DataFrame:
    """Filter for station and records date onward."""
    return df.filter(
        (F.col("NAME").isin(cities))
        & (F.col("DTG") >= F.lit(start_date))
    )


def aggregate_daily(df: DataFrame) -> DataFrame:
    """
    Aggregate multiple measurements per day into a single record per day.
    In addition to aggregating the maximum temperature, also aggregate the daily minimum temperature
    from TN_10CM_PAST_6H_10.
    """
    return (df.groupBy("DATE")
            .agg(
                F.max("TX_DRYB_10").cast("float").alias("MAX_DAILY_T"),
                F.min("TN_10CM_PAST_6H_10").cast("float").alias("MIN_TEMP")
            ))


def with_date(df: DataFrame) -> DataFrame:
    """Convert date column."""
    return df.withColumn("DATE", F.to_date(F.col("DTG"), "yyyy-MM-dd"))


def with_prev_date(df: DataFrame) -> DataFrame:
    """Add previous date column."""
    return df.withColumn("PREV_DATE", F.lag("DATE").over(date_window))


def with_date_diff(df: DataFrame) -> DataFrame:
    """Add date difference column."""
    return df.withColumn("DAY_DIFF", F.datediff(F.col("DATE"), F.col("PREV_DATE")))


def with_group(df: DataFrame) -> DataFrame:
    """
    Add group for heatwave candidates.
    Identify the start of a new group.
    Use a cumulative sum of the new_group flag to assign a group ID.
    """
    return (df
            .withColumn("NEW_GROUP", F.when((F.col("DAY_DIFF") != 1) | (F.col("PREV_DATE").isNull()), 1).otherwise(0))
            .withColumn("GROUP_ID", F.sum("NEW_GROUP").over(date_window))
    )
