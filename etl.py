""" 
Sparkify ETL process to extract JSON files from S3, tranform data and load into S3 in parquet format
"""

import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, to_timestamp


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("S3", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("S3", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """Create a Spark session to transform the data.

    Returns:
        SparkSession: Spark session.
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.1",
    ).getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """Extract and load song data.

    Args:
        spark (SparkSession): Spark session.
        input_data (string): Input path for JSON data.
        output_data (string): Output path for parquet data.
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)
    print(f"- Files read from source.")

    # extract columns to create songs table $"friend_id".isNotNull
    songs_table = df.filter(col("song_id").isNotNull()).select("song_id", "title", col("artist_id").alias("artist_id"), "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}songs")
    print(f"- Written songs table to parquet files.")

    # extract columns to create artists table
    artists_table = (
        df.filter(col("artist_id").isNotNull())
        .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
        .dropDuplicates()
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}artists")
    print(f"- Written artists table to parquet files.")


def process_log_data(spark, input_data, output_data):
    """Extract and load log data.

    Args:
        spark (SparkSession): Spark session.
        input_data (string): Input path for JSON data.
        output_data (string): Output path for parquet data.
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    print(f"- Files read from source.")

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = (
        df.filter(col("userId").isNotNull())
        .select("userId", "firstName", "lastName", "gender", "level")
        .dropDuplicates()
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}users")
    print(f"- Written users table to parquet files.")

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000))

    # extract columns to create time table
    time_table = (
        df.select("start_time")
        .withColumn("hour", hour(df.start_time))
        .withColumn("day", dayofmonth(df.start_time))
        .withColumn("week", weekofyear(df.start_time))
        .withColumn("month", month(df.start_time))
        .withColumn("year", year(df.start_time))
        .withColumn("weekday", dayofweek(df.start_time))
        .dropDuplicates()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(f"{output_data}time")
    print(f"- Written time table to parquet files.")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (
        df.join(song_df, song_df.title == df.song)
        .select(
            "start_time", col("userId").alias("user_id"), "level", "song_id", col("artist").alias("artist_id"), col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent")
        )
        .dropDuplicates()
        .withColumn("songplay_id", monotonically_increasing_id())
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn("year", year(songplays_table.start_time)).withColumn("month", month(songplays_table.start_time)).write.mode("overwrite").partitionBy("year", "month").parquet(
        f"{output_data}songplays"
    )
    print(f"- Written song plays table to parquet files.")


def main():
    """
    Sparkify ETL to load data for analytics

    1. Extract song and log data from JSON files on S3 (or local system)
    2. Transform data into analytics model
    3. Load processed data back into S3 in parquet format
    """
    print(f"--- Sparkify ETL ---\nCreate Spark session... ", end="")
    spark = create_spark_session()
    print(f"Done.")

    # Using LOCAL files for testing. Change LOCAL to S3 to use files from AWS.
    source = "LOCAL"

    # Using LOCAL files for testing. Change LOCAL to S3 to use files from AWS.
    input_data = config.get(source, "INPUT_DATA").replace("'", "")
    output_data = config.get(source, "OUTPUT_DATA").replace("'", "")

    print(f"--- Extract song data from {source=}...")
    process_song_data(spark, input_data, output_data)
    print(f"--- Song data processed.\n")

    print(f"--- Extract log data from {source=}...")
    process_log_data(spark, input_data, output_data)
    print(f"--- Log data processed.\n")

    print(f"--- ETL completed successfully.")


if __name__ == "__main__":
    main()
