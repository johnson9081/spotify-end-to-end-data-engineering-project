import json
import os
import spotipy
import boto3
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials


def lambda_handler(event, context):
    
    # Assigning Spotify Client Id and Secret Key Using Environment Variable
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    spotify_authorized = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Personal Spotify Playlist Link (Name of Playlist: Infinite)
    spotify_link = "https://open.spotify.com/playlist/7eSFqegkJI8rQdBiqxZMxn"
    spotify_URI = spotify_link.split('/')[-1]
    data = spotify_authorized.playlist_tracks(spotify_URI,fields=None,limit=50,offset=0,market=None,additional_types=('track',))
    
    # Fetching the raw data from spotify playlist and dumping into s3 bucket, to_be_processed folder with a unique file name
    client = boto3.client("s3")
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    client.put_object(
        Bucket="spotify-etl-pipeline-johnson",
        Key="raw_data/to_be_processed/" + filename,
        Body=json.dumps(data),
        )

