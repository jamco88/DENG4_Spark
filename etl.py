import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["KEYS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["KEYS"]['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process Song Data
    
    Pulls down the raw json song data from S3
    Creates two separate dimension tables- songs and artists
    Saves these locally
    
    Args:
        spark: SparkSession Object
        input_data: URL to source s3 bucket
        output_data: destination s3 bucket
    
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df["song_id", "title", "artist_id", "artist_name", "year", "duration"]
    print("songs_table created")
    #remove duplicates
    songs_table = songs_table.drop_duplicates()    
    print("songs_table duplicates dropped")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"), "overwrite")
    print("songs.parquet created and saved locally.")

    # extract columns to create artists table
    artists_table = df["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table.drop_duplicates()
    print("artists table created and duplicates dropped")
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), "overwrite")
    print("artists.parquet created and saved locally")
    print("Finished processing Songs.json")


def process_log_data(spark, input_data, output_data):
    """Process log json data
    
    Pulls the raw json log data from s3 and saves 3 tables in parquet format to local
    Creates fact table songplays, dimension tables users and time
    songplays fact table takes song.parquet file as input
    
    Args:
        spark: SparkSession Object
        input_data: s3 bucket url
        output_data: destination s3 bucket url
    
    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(col("page")=="Nextsong")

    # extract columns for users table
    users_table = df["userId", "firstName", "lastName", "gender", "level", "ts"]
    # sort table on timestamp then drop timestamp
    users_table = users_table.orderBy("ts", ascending=False).drop("ts")
    #drop duplicates
    users_table = users_table.drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users.parquet"), "overwrite")
    
    print("users.parquet file created and saved locally")

  
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    get_weekday = udf(lambda x: x.weekday())
    get_week = udf(lambda x: datetime.isocalendar(x)[1])
    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_year = udf(lambda x: x.year)
    get_month = udf(lambda x: x.month)
    
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # derive further columns from new start_time column
    df = df.withColumn("weekday", get_weekday(df.start_time))
    df = df.withColumn("week", get_week(df.start_time))
    df = df.withColumn("hour", get_hour(df.start_time))
    df = df.withColumn("day", get_day(df.start_time))
    df = df.withColumn("year", get_year(df.start_time))
    df = df.withColumn("month", get_month(df.start_time))
    
    # extract columns to create time table
    time_table = df["start_time", "weekday", "hour", "day", "week","month", "year"]
    
    #remove duplicates
    time_table = time_table.drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"), "overwrite")
    
    print("time.parquet file written")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist))
    df = df.withColumn("songplay_id", monotonically_increasing_id())                                 
    songplays_table = df["songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "year", "month"]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays.parquet"), "overwrite")
    print("songplays.parquet file written, job done!")
                                 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/" 
    output_data = "s3://jcsparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
