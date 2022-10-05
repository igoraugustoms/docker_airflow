import boto3
from pyspark import SparkContext, SparkConf
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
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
  exists_folder=exists_subfolder("datalake-igti-igor", "spotify/ref/tracks/", client)
  df_raw=read_df_raw()
  write_df_ref(df_raw, exists_folder)
  print("***Status job: Done***")


def read_df_raw():
  df_raw_tracks=(
      spark
      .read
      .format("json")
      .option("inferSchema", "true")
      .option("multiline", "true")
      .load("s3a://datalake-igti-igor/spotify/raw/tracks/")
    )
  df_raw_tracks_2=(
    df_raw_tracks
    .withColumn("artists", explode("artists"))
    .withColumn("available_markets", explode("available_markets"))
    .withColumn('ranking_date', to_date("date_ranking", "yyyy-MM-dd"))
    .select(
      col("album.album_type").alias('album_type'),
      col("album.href").alias('album_href'),
      col("album.id").alias('album_id'),
      col("album.name").alias('album_name'),
      col("album.release_date").alias('album_release_date'),
      col("album.total_tracks").alias('album_total_tracks'),
      col("album.uri").alias('album_uri'),
      col("artists.href").alias('artists_href'),
      col("artists.id").alias('artists_id'),
      col("artists.name").alias('artists_name'),
      col("artists.type").alias('artists_type'),
      col("artists.uri").alias('artists_uri'),
      "disc_number",
      "duration_ms",
      "explicit",
      "external_ids.isrc",
      "external_urls.spotify",
      "href",
      "id",
      "is_local",
      "name",
      "popularity",
      "preview_url",
      "track_number",
      "type",
      "uri",
      "position",
      'ranking_date')
     )


  return df_raw_tracks_2
  


def write_df_ref(df_raw, exists_folder):
  df_raw=df_raw
  exists_subfolder=exists_folder
  #check if exists ref, join and write
  if(exists_subfolder):
    df_ref=(
      spark
      .read
      .format("parquet")
      .load("s3a://datalake-igti-igor/spotify/ref/tracks")
    )

    new_df=df_raw.join(df_ref, ['position', 'ranking_date'],how='left_anti')

    (
      new_df
      .write
      .partitionBy("ranking_date")
      .mode("append")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/ref/tracks")
    )
  else:
    (
      df_raw
      .write
      .partitionBy("ranking_date")
      .mode("append")
      .format("parquet")
      .save("s3a://datalake-igti-igor/spotify/ref/tracks") 
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
    