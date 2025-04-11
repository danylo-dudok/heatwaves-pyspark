"""Heatwave detection module."""

from pyspark.sql import DataFrame, functions as F


def with_maximum_flags(df: DataFrame) -> DataFrame:
    """Add daily maximum temperature flags."""
    return (df
            .withColumn("is25", F.when(F.col("MAX_DAILY_T") >= 25, True).otherwise(False))
            .withColumn("is30", F.when(F.col("MAX_DAILY_T") >= 30, True).otherwise(False))
        )


def filter_wave_candidates(df: DataFrame) -> DataFrame:
    """Filter for heatwave candidates."""
    return df.filter(F.col("is25") == F.lit(True))


def group_metrics(df: DataFrame) -> DataFrame:
    """Group by date and calculate metrics."""
    return (
        df.groupBy("GROUP_ID").agg(
            F.min("DATE").alias("FROM_DATE"),
            F.max("DATE").alias("TO_DATE"),
            F.count("*").alias("DURATION"),
            F.sum(
                F.when(F.col("is30") == True, 1).otherwise(0)
            ).alias("TROPICAL_DAYS"),
            F.max("MAX_DAILY_T").alias("MAX_TEMPERATURE"),
        )
    )


def filter_heatwaves(df: DataFrame) -> DataFrame:
    """Filter for heatwaves."""
    return df.filter((F.col("DURATION") >= 5) & (F.col("TROPICAL_DAYS") >= 3))
