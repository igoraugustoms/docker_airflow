import boto3
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, collect_list, concat_ws


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
    client=connect_aws_s3()
    exists_folder=exists_subfolder("datalake-igti-igor", "spotify/exp/f_artist/", client)
    df_ref_artist, df_ref_track=read_ref()
    df_f_artist=select_f_artist(df_ref_artist, df_ref_track)
    write_df(df_f_artist, exists_folder)
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
def select_f_artist(df_artist, df_ref_track):
    df_ref_track=df_ref_track
    df_artist=df_artist
    df_ranking_artist=(
        df_artist
        .select(
            'id',
            'total',
            'genres'
        )    
    ).distinct()
    df_artist_agg=(
        df_ranking_artist
        .groupby('id', 'total')
        .agg(concat_ws(", ", collect_list(df_ranking_artist.genres)).alias('genres'))
    )
    
    # join and bring column date and position
    df_f_artist=(
        df_artist_agg
        .join(df_ref_track.select(col("ranking_date").alias("ranking_date"), col("artists_id").alias("artists_id"), col("position").alias("position")), col("id") == col("artists_id")))
    
    
    return df_f_artist

def write_df(df, exists_folder):
    df_ranking=df
    exists_subfolder=exists_folder
    if(exists_subfolder):
        df_exp=(
          spark
          .read
          .format("parquet")
          .load("s3a://datalake-igti-igor/spotify/exp/f_artist")
        )

        new_df=df_ranking.join(df_exp, ['position', 'ranking_date', 'id'],how='left_anti')

        (
          new_df
          .write
          .partitionBy("ranking_date")
          .mode("append")
          .format("parquet")
          .save("s3a://datalake-igti-igor/spotify/exp/f_artist")
        )
    else:
      (
      df_ranking
      .write
      .partitionBy("ranking_date")
      .mode("append")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/exp/f_artist") 
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
    