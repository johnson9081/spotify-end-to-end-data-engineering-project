import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

# Album Transformation Function
def album_transformation(data):
    album_list = []
    for i in range(len(data['items'])):
        album_dict = {  'album_id': data['items'][i]['track']['album']['id'],
                        'name': data['items'][i]['track']['album']['name'],
                        'release_date': data['items'][i]['track']['album']['release_date'],
                        'total_tracks': data['items'][i]['track']['album']['total_tracks'],
                        'url': data['items'][i]['track']['album']['external_urls']['spotify']}
        album_list.append(album_dict)
    return album_list

# Artist Transformation Function
def artist_transformation(data):
    artist_list = []
    for i in range(len(data['items'])):
        for j in range(len(data['items'][i]['track']['artists'])):
            artist_dict = {'artist_id': data['items'][i]['track']['artists'][j]['id'],
                         'artist_name': data['items'][i]['track']['artists'][j]['name'],
                         'external_url': data['items'][i]['track']['artists'][j]['href']}
            artist_list.append(artist_dict)
    return artist_list

# Songs Transformation Function    
def songs_transformation(data):
    songs_list = []
    for val in data['items']:
        song_id = val['track']['id']
        song_name = val['track']['name']
        song_duration = val['track']['duration_ms']
        song_url = val['track']['external_urls']['spotify']
        song_popularity = val['track']['popularity']
        song_added = val['added_at']
        album_id = val['track']['album']['id']
        artist_id = val['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id,'song_name': song_name, 'duration_ms': song_duration, 'url': song_url, 'popularity': song_popularity,
                       'song_added': song_added, 'album_id': album_id, 'artist_id': artist_id}
        songs_list.append(song_element)
    return songs_list

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotify-etl-pipeline-johnson"
    Key = "raw_data/to_be_processed/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_key = file['Key']
        print()
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket,Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album = album_transformation(data)
        artist = artist_transformation(data)
        songs = songs_transformation(data)
        
        #Album DataFrame
        album_df = pd.DataFrame.from_dict(album)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'],format='mixed')
        
        #Artist DataFrame
        artist_df = pd.DataFrame.from_dict(artist)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        #Songs DataFrame
        songs_df = pd.DataFrame.from_dict(songs)
        songs_df['song_added'] = pd.to_datetime(songs_df['song_added'],format='mixed')
        
        songs_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        songs_df.to_csv(song_buffer,index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=songs_key,Body=song_content)
    
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
        
        artist_key = "transformed_data/artists_data/artists_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_resource = {'Bucket': Bucket,'Key': key}
        s3_resource.meta.client.copy(copy_resource, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
