from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import boto3


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
  exists_folder=exists_subfolder("datalake-igti-igor", "spotify/ref/tracks_features/", client)
  schema=define_schema()
  df_raw=read_df_raw(schema)
  write_df_ref(df_raw, exists_folder, schema)
  print("***Status Job: Done***")


def define_schema():
  schema="""
    accousticness FLOAT,
    analysis_url STRING,
    danceability FLOAT,
    duration_ms INT,
    energy FLOAT,
    id STRING,
    instrumentalness FLOAT,
    key INT,
    liveness FLOAT,
    loudness FLOAT,
    mode FLOAT,
    speechiness FLOAT,
    tempo FLOAT,
    time_signature INT,
    track_href STRING,
    type STRING,
    uri STRING,
    valence FLOAT"""
  return schema


def read_df_raw(schema):
  schema=schema
  df=(
    spark
    .read
    .format("json")
    .schema(schema)
    .load("s3a://datalake-igti-igor/spotify/raw/tracks_features/")
  )
  return df


def write_df_ref(df_raw, exists_subfolder, schema):
  df_raw = df_raw
  exists_subfolder=exists_subfolder
  schema=schema
  if(exists_subfolder):
    df_ref=(
    spark
    .read
    .format("parquet")
    .schema(schema)
    .load("s3a://datalake-igti-igor/spotify/ref/tracks_features")
    )

    new_df=df_raw.join(df_ref, how='left_anti', on='id')

    (
        new_df
        .write
        .mode("append")
        .format("parquet")
        .save("s3a://datalake-igti-igor/spotify/ref/tracks_features")
    )
  else:
    (
        df_raw
       .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://datalake-igti-igor/spotify/ref/tracks_features")
    ) 
 

def connect_aws_s3():
    #Estabeleco conexao com s3
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
    