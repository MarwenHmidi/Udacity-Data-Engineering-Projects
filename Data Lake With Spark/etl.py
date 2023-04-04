import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Write the data into parquet files that will be loaded on S3 
    after loading the song data dataset and extracting 
    data for songs and artists tables.
    """
    
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])


    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude') \
                      .dropDuplicates(subset=['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/' + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    
    """
    Extract columns from the log data dataset after loading it's data.
    Reads the song data and log data datasets for users and time
    tables and extract fields for the songplays table with the data.
    Store the data in parquet files that will be loaded into S3.
    """
    
    
    # get filepath to log data file
    log_data = log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(['userId', 'firstName as first_name', 'lastName as last_name', 'gender', 'level', 'ts']).dropDuplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet', partitionBy = ['userId'])


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates(subset=['start_time'])

    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/' + 'time.parquet',partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    #giving alias to dataframes to use them on the join query
    df = df.alias('df')
    song_df = song_df.alias('song_df')
    # extract columns from joined song and log datasets to create songplays table 
    joined_song_log_table = df.join(song_df, (col('df.artist') == col('song_df.artist_name')) \
                                    & (col('df.song') == col('song_df.title')) \
                                    & (col('df.length') == col('song_df.duration')), 'inner')

    songplays_table = joined_song_log_table.select(
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('df.sessionId').alias('session_id'),
        col('df.location').alias('location'), 
        col('df.userAgent').alias('user_agent'),
        col('df.datetime').alias('start_time'),
        col('df.userId').alias('user_id'),
        col('df.level').alias('level'),
        year('df.datetime').alias('year'),
        month('df.datetime').alias('month')) \
        .withColumn('songplay_id',  F.monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/' + 'songplays.parquet',partitionBy=['year', 'month'])



def main():
    """
    Perform the following steps:
    1. Get or create a spark session.
    2. Extract data, transform it,
    which will then be written to parquet files.
    3 Load the parquet files into s3 bucket.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-sparkify-output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
