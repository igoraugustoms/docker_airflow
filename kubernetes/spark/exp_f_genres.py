import boto3
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col


aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()


def main():
    df_ref_artist, df_ref_track=read_ref()
    df_f_genre=select_f_genre(df_ref_artist, df_ref_track)
    write_df(df_f_genre)
    print("***Status job: Done***")


def read_ref():
    df_ref_artist=(
        spark
        .read
        .format("parquet")
        .load("s3a://datalake-igti-igor/spotify/ref/artists")
    )
    
    df_ref_tracks=(
        spark
        .read
        .format("parquet")
        .load("s3a://datalake-igti-igor/spotify/ref/tracks")
    )
    
    return df_ref_artist, df_ref_tracks

def select_f_genre(df_artist, df_ref_track):
    df_ref_track=df_ref_track
    df_artist=df_artist
    df_daily=(
        df_ref_track
        .select(
            'id',
            'artists_id',
            'ranking_date'
        )    
    ).distinct()

    df_artist_unique=(
        df_artist
        .select(
            col("id").alias("id_artist_unique"), 
            col("genres").alias("genre")
        )
        .distinct()
    )
   
    df_joined=(
            df_daily
            .join(df_artist_unique, col("id_artist_unique") == col("artists_id")))

    df_artist_agg=(
        df_joined
        .groupby('genre', 'ranking_date')
        .count()
    )
    
       
    return df_artist_agg

def write_df(df):
    df_genre=df
    (
      df_genre
      .write
      .mode("overwrite")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/exp/f_genre") 
    )
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")



    main()
    spark.stop()
    