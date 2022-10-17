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
#sc.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true') 
#sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.sa-east-2.amazonaws.com') 


def main():
  client=connect_aws_s3()
  exists_folder=exists_subfolder("datalake-igti-igor", "spotify/exp/f_track_statistics/", client)
  df_ref_tracks,df_ref_tracks_features=read_ref()
  df_statistics=select_statistics(df_ref_tracks, df_ref_tracks_features)
  write_df(df_statistics, exists_folder)
  print("***Status job: Done***")


def read_ref():
  df_ref_tracks=(
    spark
    .read
    .format("parquet")
    .load("s3a://datalake-igti-igor/spotify/ref/tracks")
    )
  df_ref_tracks_features=(
    spark
    .read
    .format("parquet")
    .load("s3a://datalake-igti-igor/spotify/ref/tracks_features")
    )
  return df_ref_tracks, df_ref_tracks_features


def select_statistics(df_tracks, df_tracks_features):
  df_tracks=df_tracks
  df_tracks_features=df_tracks_features
  df_statistics=(
    df_tracks_features
    .select(
      'acousticness',
      'danceability',
      'duration_ms',
      'energy',
      'id',
      'instrumentalness',
      'liveness',
      'loudness',
      'mode',
      'speechiness',
      'valence',
      'tempo',
      'time_signature')
    )
    # join and bring column date and position
  df_f_statistics=(
    df_statistics
    .join(
      df_tracks
      .select(
        col("ranking_date").alias("ranking_date"), 
        col("id").alias("track_id"), 
        col("position").alias("position")
        ), 
      col("track_id") == col("id")
      )
  ).distinct()
    
  return df_f_statistics


def write_df(df, exists_folder):
  df_statistics=df
  exists_subfolder=exists_folder
  if(exists_subfolder):
    df_exp=(
      spark
      .read
      .format("parquet")
      .load("s3a://datalake-igti-igor/spotify/exp/f_track_statistics")
      )

    new_df=df_statistics.join(df_exp, ['position', 'ranking_date'],how='left_anti')

    (
      new_df
      .write
      .partitionBy("ranking_date")
      .mode("append")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/exp/f_track_statistics")
    )
     
  else:
    (
      df_statistics
      .write
      .partitionBy("ranking_date")
      .mode("append")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/exp/f_track_statistics") 
    )
     

def connect_aws_s3():
    #Connect to my AWS account
    client = boto3.client('s3', region_name='us-east-2', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    return client


def exists_subfolder(folder_name, subfolder, client):
  #Checks if subfolder ref exists
  try:
    bucket = 'datalake-igti-igor'
    client=client
    prefix =  folder_name 
    subfolder=subfolder
    exists=False
    result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    for o in result.get('CommonPrefixes'):
      if(subfolder == o.get('Prefix')):
        exists=True
    return exists
  except Exception as e:
    exists=False
    return exists




if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")



    main()
    spark.stop()
    