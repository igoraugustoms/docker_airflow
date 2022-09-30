from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import os
import requests
import json
import spotipy
import spotipy.util as util 
from spotipy.oauth2 import SpotifyClientCredentials
import boto3

DATES=[
      ("2022-04-01","2022-04-30"),
      ("2022-05-01","2022-05-31"),
      ("2022-06-01","2022-06-30"),
      ("2022-07-01","2022-07-31"),
      ("2022-08-01","2022-08-31"),
      ("2022-09-01","2022-09-30")
]

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
    get_archive_data(sp, client)

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

def get_archive_data(sp, client):
    sp=sp
    client=client
    for date in DATES:
        #request para url com dados historicos
        url="https://spotifyplaylistarchive.com/api/playlists/37i9dQZEVXbMXbN3EUUhlg/snapshots?sinceDate="+date[0]+"&untilDate="+date[1]
        my_request=requests.get(url)
        responses=json.loads(my_request.content)

        #pego o commitSHA para fazer o segundo request, o que trazos dados das listas de musicas, dia a dia
        commit_shas=[response['commitSha'] for response in responses]
        for commit_sha in commit_shas:
            final_url="https://raw.githubusercontent.com/mackorone/spotify-playlist-archive/"+commit_sha+"/playlists/pretty/37i9dQZEVXbMXbN3EUUhlg.json"
            my_second_request=requests.get(final_url)
            final_responses=json.loads(my_second_request.content)

        #Pego a URL de cada musica e busco as informacoes oficiais dela
            position=1
            for track in final_responses['tracks']:
                track_url=track['url']
                date_ranking=track['added_at']
                track_information=sp.track(track_url)
                track_information['Position']=position
                client.put_object(Body=json.dumps(track_information), Bucket='datalake-igti-igor', Key='spotify/raw/tracks/'+date_ranking[0:10]+'/musica'+str(position)+'.json')
                track_features=sp.audio_features(track_url)
                client.put_object(Body=json.dumps(track_features[0]), Bucket='datalake-igti-igor', Key='spotify/raw/tracks_features/'+date_ranking[0:10]+'/feature'+str(position)+'.json')
                for artist in track_information['artists']:
                    artist_informartion=sp.artist(artist['id'])
                    client.put_object(Body=json.dumps(artist_informartion), Bucket='datalake-igti-igor', Key='spotify/raw/artists/'+date_ranking[0:10]+'/'+artist_informartion['id']+'_____'+str(position)+'.json')    
                position+=1
        print('Done from '+date[0]+' until '+date[1])


if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Repartition Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    main()
    spark.stop()