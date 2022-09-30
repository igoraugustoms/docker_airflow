from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import json
import os
from datetime import datetime
import spotipy
import spotipy.util as util 
from spotipy.oauth2 import SpotifyClientCredentials
import boto3


aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
client_id_spotipy= os.environ['CLIENT_ID_SPOTIPY']
client_secret_spotipy= os.environ['CLIENT_SECRET_SPOTIPY']


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
    sp=connect_spotify()
    client=connect_aws_s3()
    get_data(sp, client)

def connect_spotify():
    #Estabeleco conexao com api oficial spotify
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id_spotipy, client_secret=client_secret_spotipy)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    return sp

def connect_aws_s3():
    #Estabeleco conexao com s3
    client = boto3.client('s3', region_name='us-east-2', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    return client

def get_data(sp, client):
    sp=sp
    client=client
    #Faco requisicao na API Oficial e armazeno os IDs das musicas no ranking
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMXbN3EUUhlg"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_ids = [track_data["track"]["id"] for track_data in sp.playlist_tracks(playlist_URI)["items"]]
    #Com o ID das musicas, eu busco a informacao completa delas, artistas e features    
    position=1
    for track_id in track_ids:
        date_ranking=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        track_information=sp.track(track_id)
        track_information['Position']=position
        client.put_object(Body=json.dumps(track_information), Bucket='datalake-igti-igor', Key='spotify/raw/tracks/'+date_ranking[0:10]+'/musica'+str(position)+'.json')
        track_features=sp.audio_features(track_id)
        client.put_object(Body=json.dumps(track_features[0]), Bucket='datalake-igti-igor', Key='spotify/raw/tracks_features/'+date_ranking[0:10]+'/feature'+str(position)+'.json')
        for artist in track_information['artists']:
            artist_informartion=sp.artist(artist['id'])
            client.put_object(Body=json.dumps(artist_informartion), Bucket='datalake-igti-igor', Key='spotify/raw/artists/'+date_ranking[0:10]+'/'+artist_informartion['id']+'_____'+str(position)+'.json')    
        position+=1    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")



    main()
    spark.stop()
    