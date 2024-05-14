# Import the necessary modules
from pyspark.sql import SparkSession, DataFrame
import pyspark as spark
from pyspark.sql.functions import col, max, avg, min, desc, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


def main():
    """Main ETL Script

    :return: None
    """

    # -- Load Data --

    # Create a SparkSession
    spark = SparkSession.builder.appName("DA Task").getOrCreate()

    ratings_schema = "UserID int, MovieID int, Rating int, Timestamp long"
    ratings_df = spark.read.csv(
        "input_data/ratings.dat", sep="::", schema=ratings_schema, header=False
    )

    movies_schema = "MovieID int, Title string, Genres string"
    movies_df = spark.read.csv(
        "input_data/movies.dat", sep="::", schema=movies_schema, header=False
    )

    # -- Execute --

    agg_ratings_df = aggregate_ratings(ratings_df)
    movies_clean_df = clean_movies(movies_df)

    # Join. Task doesn't specify so default to left join
    movies_ratings_df = movies_clean_df.join(agg_ratings_df, "MovieId", "left")
    top_3_ratings_df = top_3_movies_per_user(ratings_df)

    # -- Save --
    # Save the files
    movies_ratings_df.write.parquet("output_data/q2.parquet", mode="overwrite")
    top_3_ratings_df.write.parquet("output_data/q3.parquet", mode="overwrite")

    # Stop the SparkSession
    spark.stop()


# All movies - max,min,average rating
def aggregate_ratings(input: DataFrame) -> DataFrame:
    """
    Returns max, avg and min rating per MovieId
    """
    # Do aggregates on smaller data pre-join
    agg_ratings_df = input.groupBy("MovieId").agg(
        max("Rating").alias("max_rating"),
        avg("Rating").alias("avg_rating"),
        min("Rating").alias("min_rating"),
    )
    return agg_ratings_df


def clean_movies(input: DataFrame) -> DataFrame:
    """Data file's README explicitly states:
    'Some MovieIDs do not correspond to a movie due to accidental duplicate entries and/or test entries'
    so cleaning up
    """
    output = input.dropDuplicates(["MovieId"])
    return output


def top_3_movies_per_user(input: DataFrame) -> DataFrame:
    """
    Note: Returned movies not necessarily deterministic due to using row_number
        to handle equal ratings
    """
    # Each UserID's top 3 movies
    window_spec = Window.partitionBy("UserId").orderBy(desc("Rating"))
    # Lots of matching ratings so need row_number rather than rank
    top_3_ratings_df = input.select(
        "UserID", "MovieID", "Rating", row_number().over(window_spec).alias("rank")
    ).filter(col("rank") <= 3)
    return top_3_ratings_df


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
