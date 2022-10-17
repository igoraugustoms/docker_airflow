import boto3
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col


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

#dim_artist
def main():
  df_ref=read_ref()
  df_artist=select_artist(df_ref)
  write_df(df_artist)
  print("***Status job: Done***")
    

def read_ref():
  df_ref=(
    spark
    .read
    .format("parquet")
    .load("s3a://datalake-igti-igor/spotify/ref/artists")
  )
  return df_ref

def select_artist(df):
  df=df
  df_artist=(
    df
    .select(
      "href",
      "id",
      "name",
      "popularity",
      "type",
      "uri")
      ).distinct()
  return df_artist

def write_df(df_artist):
  df_artist=df_artist(
    df_artist
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3a://datalake-igti-igor/spotify/exp/dim_artist")
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
    