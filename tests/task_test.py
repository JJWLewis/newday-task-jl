from src.task import clean_movies, aggregate_ratings
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.functions import col


def test_aggregate_ratings_happy(spark):
    """Happy path test"""

    # Prep
    ratings_df = spark.createDataFrame(
        schema=["MovieId", "Rating"],
        data=[(1, 3), (1, 4), (1, 5), (2, 1), (2, 2), (2, 3)],
    )
    # Expected data
    expected_df = spark.createDataFrame(
        schema=["MovieId", "max_rating", "avg_rating", "min_rating"],
        data=[(1, 5.0, 4.0, 3.0), (2, 3.0, 2.0, 1.0)],
    )
    expected_df = expected_df.select(
        col("MovieId"),
        col("max_rating").cast(LongType()),
        col("avg_rating").cast(DoubleType()),
        col("min_rating").cast(LongType()),
    )
    # Execute
    result_df = aggregate_ratings(ratings_df)

    # Assert
    assert_df_equality(result_df, expected_df)


def test_aggregate_ratings_nulls_not_0(spark):
    """
    Validate Spark's max,min,avg respect nulls as 0 not empty
    i.e. the row is still counted in the avg, and ignored in min/max
    """

    # Prep

    # Expected data

    # Execute

    # Assert


def test_clean_movies_happy(spark):
    """
    Check the deduplication based on MovieId correctly picks lower id
    """

    # Prep
    movies_df = spark.createDataFrame(
        schema=["MovieId", "Title"],
        data=[(1, "Toy Story (1995)"), (2, "Jumanji (1995)"), (2, "Not Jumanji")],
    )

    # Expected data
    expected_df = spark.createDataFrame(
        schema=["MovieId", "Title"],
        data=[(1, "Toy Story (1995)"), (2, "Jumanji (1995)")],
    )

    # Execute
    result_df = clean_movies(movies_df)

    # Assert
    assert_df_equality(result_df, expected_df)
