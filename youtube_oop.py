import googleapiclient.discovery
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from googleapiclient.errors import HttpError








class Youtube:

    def __init__(self, channel_id):
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.youtube_api_key = "AIzaSyD3zMC1xjxdpNTITczCljFwOBbCoRn9Hhc"

        self.youtube = googleapiclient.discovery.build(self.api_service_name, self.api_version, developerKey=self.youtube_api_key)

        self.channel_id = channel_id

        self.db = pymysql.connect(
        host="localhost",
        user="root",
        password="1234",
        connect_timeout=10

        )

        self.mysql = "mysql+pymysql://root:1234@localhost/youtube"
        self.engine = create_engine(self.mysql)

        self.connect = self.db.cursor()

    # returns channel details
    def channel(self):
        try:
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=self.channel_id
            )
            channel_response = request.execute()

            if 'items' in channel_response and len(channel_response['items']) > 0:
                channel_data = {
                    "channel_id": channel_response["items"][0]["id"],
                    "channel_name": channel_response["items"][0]["snippet"]["title"],
                    "description": channel_response["items"][0]["snippet"]["description"],
                    "date": channel_response["items"][0]["snippet"]["publishedAt"],
                    "upload_id": channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                    "views": channel_response["items"][0]["statistics"]["viewCount"],
                    "subscribers": channel_response["items"][0]["statistics"]["subscriberCount"],
                    "videos": channel_response["items"][0]["statistics"]["videoCount"]
                }
                df = pd.DataFrame(channel_data, index=[0])
                df['date'] = (pd.to_datetime(df['date'], utc=True)).dt.date
                df['date'] = pd.to_datetime(df['date'])
            else:
                df = pd.DataFrame()

        except Exception as e:
            df = pd.DataFrame()

        return df

    # returns video ids
    def video_ids(self): 
        video_ids = []
        response = self.youtube.channels().list(part="contentDetails", id=self.channel_id).execute()
        
        # Handle missing keys
        try:
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        except KeyError as e:
            print(f"KeyError: {e}")
            return []

        next_page_token = None

        while True:
            playlist_response = self.youtube.playlistItems().list(
                part="snippet",
                maxResults=50,
                pageToken=next_page_token,
                playlistId=playlist_id
            ).execute()

            for item in playlist_response.get('items', []):
                video_ids.append(item['snippet']['resourceId']['videoId'])
            
            next_page_token = playlist_response.get('nextPageToken')

            if not next_page_token:
                break
        
        return video_ids


    
    # returns video details
    def video_details(self, video_ids):  # returns video details dataframe
        video_details = []
        
        for video_id in video_ids:
            try:
                response = self.youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()

                for i in response['items']:
                    duration_seconds = parse_duration(i['contentDetails']['duration']).total_seconds()
                    data = {
                        "channel_id": i['snippet']['channelId'],
                        "video_id": i['id'],
                        "video_name": i['snippet']['title'],
                        "description": i['snippet']['description'],
                        "published_at": i['snippet']['publishedAt'],
                        "views": i['statistics']['viewCount'],
                        "likes": i['statistics'].get('likeCount'),
                        "dislikes": i.get('dislikeCount'),
                        "favorites": i['statistics']['favoriteCount'],
                        "comments": i['statistics'].get('commentCount'),
                        "duration": i['contentDetails']['duration'],
                        "thumbnail": i['snippet']['thumbnails']['default']['url'],
                        "caption_status": i['contentDetails']['caption'],
                        "tags": ",".join(i['snippet'].get('tags', ["na"]))
                    }
                    video_details.append(data)
            except Exception as e:
                print(f"An error occurred while fetching details for video ID {video_id}: {e}")
        
        # Create a DataFrame only if video_details is not empty
        if video_details:
            df = pd.DataFrame(video_details)
            df['published_at'] = pd.to_datetime(df['published_at'], format='%Y-%m-%dT%H:%M:%SZ')
            df['duration'] = pd.to_timedelta(df['duration'])
            df['duration'] = df['duration'].apply(lambda x: str(x).split()[-1] if 'days' in str(x) else str(x))
            df['duration'] = pd.to_timedelta(df['duration'])
        else:
            df = pd.DataFrame()  # Return an empty DataFrame if no video details were collected
        
        return df

    # returns comment details dataframe
    def comment_details(self, video_ids):  
        comment_details = []

        for video_id in video_ids:
            try:
                response = self.youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=20).execute()
                
                for item in response.get('items', []):
                    data = {
                        'comment_id': item['id'],
                        'video_id': item['snippet']['videoId'],
                        'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_details.append(data)
            except Exception as e:
                print(f"An error occurred while fetching comments for video ID {video_id}: {e}")

        # Create DataFrame
        df = pd.DataFrame(comment_details)
        
        # Check if 'published_at' column exists before trying to convert it
        if 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at'], format='%Y-%m-%dT%H:%M:%SZ')
        else:
            print("Warning: 'published_at' column is missing from the comments DataFrame")
        
        return df


    
    

    # sql tables

    def channel_table(self, channel_details):

        self.connect.execute('create database if not exists youtube')
        self.connect.execute('use youtube')

        # connect.execute('drop table if exists comment')
        # connect.execute('drop table if exists video')
        # connect.execute('drop table if exists channel')
        self.connect.execute('''create table if not exists channel( channel_id VARCHAR(50) primary key,
                                        channel_name VARCHAR(1000),description VARCHAR(1000), date DATETIME,
                                        upload_id VARCHAR(50), views BIGINT,
                                        subscribers BIGINT, videos BIGINT)''')

        self.db.commit()
        channel_details.to_sql('channel',self.engine,if_exists='append',index=False)


    def video_table(self,videos_details):
        self.connect.execute('create database if not exists youtube')
        self.connect.execute('use youtube')


        self.connect.execute('''create table if not exists video( channel_id VARCHAR(50),video_id VARCHAR(50) PRIMARY KEY ,
                                        video_name VARCHAR(1000) ,description VARCHAR(10000),published_at VARCHAR(50),views BIGINT,
                                        likes BIGINT,dislikes BIGINT,favorites BIGINT,comments BIGINT,
                                        duration VARCHAR(50),thumbnail VARCHAR(100),caption_status VARCHAR(50),tags VARCHAR(1000),FOREIGN KEY (channel_id) REFERENCES channel(channel_id))''')


        self.db.commit()
        videos_details.to_sql('video',self.engine, if_exists='append',index=False)


    def comment_table(self, comments_details):
        self.connect.execute('create database if not exists youtube')
        self.connect.execute('use youtube')
        self.connect.execute('''create table if not exists Comment( video_id VARCHAR(50), comment_id VARCHAR(50),
                                       text TEXT,author VARCHAR(1000),published_at DATETIME, FOREIGN KEY (video_id) REFERENCES video(video_id))''')
        self.db.commit()
        comments_details.to_sql('comment', self.engine, if_exists='append', index=False)

        self.engine.dispose()

        ('--------------------------')
        

    





