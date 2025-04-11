"""Coldwave detection module."""

from pyspark.sql import DataFrame, functions as F


def with_minimum_flags(df: DataFrame) -> DataFrame:
    """Add daily temperature flags for coldwaves."""
    return (df.withColumn("is0", F.when(F.col("MAX_DAILY_T") < 0, True).otherwise(False))
            .withColumn("isHighFrost", F.when(F.col("MIN_TEMP") < -10, True).otherwise(False))
        )


def filter_wave_candidates(df: DataFrame) -> DataFrame:
    """Filter for coldwave candidates."""
    return df.filter(F.col("is0") == F.lit(True))


def group_metrics(df: DataFrame) -> DataFrame:
    """Group by GROUP_ID and calculate coldwave metrics."""
    return (
        df.groupBy("GROUP_ID").agg(
            F.min("DATE").alias("FROM_DATE"),
            F.max("DATE").alias("TO_DATE"),
            F.count("*").alias("DURATION"),
            F.sum(F.when(F.col("isHighFrost") == True, 1).otherwise(0)).alias("HIGH_FROST_DAYS"),
            F.min("MIN_TEMP").alias("MIN_TEMPERATURE"),
        )
    )


def filter_heatwaves(df: DataFrame) -> DataFrame:
    """Filter for coldwaves."""
    return df.filter((F.col("DURATION") >= 5) & (F.col("HIGH_FROST_DAYS") >= 3))
