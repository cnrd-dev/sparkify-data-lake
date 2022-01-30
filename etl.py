import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("S3", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("S3", "AWS_SECRET_ACCESS_KEY")
# os.environ["AWS_SESSION_TOKEN"] = config.get("AWS", "AWS_SESSION_TOKEN")


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:2.7.0",
    ).getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table $"friend_id".isNotNull
    songs_table = df.filter(col("song_id").isNotNull()).select("song_id", "title", col("artist_id").alias("artist_id"), "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(f"{output_data}/songs")

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
    artists_table.write.parquet(f"{output_data}/artists.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

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
    users_table.write.parquet(f"{output_data}/users.parquet")

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000))

    df = df.filter(col("userId").isNotNull()).select(col("userId").alias("user_id"), col("firstName").alias("first_name"), col("lastName").alias("last_name"), "gender", "level").dropDuplicates()

    # extract columns to create time table
    time_table = (
        df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
        .withColumn("hour", hour(df.start_time))
        .withColumn("day", dayofmonth(df.start_time))
        .withColumn("week", weekofyear(df.start_time))
        .withColumn("month", month(df.start_time))
        .withColumn("year", year(df.start_time))
        .withColumn("weekday", dayofweek(df.start_time))
        .dropDuplicates()
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f"{output_data}/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}/songs.parquet")

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
    songplays_table.write.partitionBy(year(songplays_table.start_time), month(songplays_table.start_time)).parquet(f"{output_data}/songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3nanocnrd/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
