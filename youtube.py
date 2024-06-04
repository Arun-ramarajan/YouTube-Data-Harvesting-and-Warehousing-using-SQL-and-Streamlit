import googleapiclient.discovery
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st


api_service_name = "youtube"
api_version = "v3"
youtube_api_key = "youtube_api_key"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=youtube_api_key)

def fetch_details():
    #to fetch the channel details
    def channel(channel_id):
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        channel_response = request.execute()


        channel_data = {
            "channel_id": channel_response["items"][0]["id"],
            "channel_name": channel_response["items"][0]["snippet"]["title"],
            "description": channel_response["items"][0]["snippet"]["description"],
            #"date": channel_response["items"][0]["snippet"]["publishedAt"],
            "upload_id": channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
            "views_count": channel_response["items"][0]["statistics"]["viewCount"],
            "sub_count": channel_response["items"][0]["statistics"]["subscriberCount"],
            "video_count": channel_response["items"][0]["statistics"]["videoCount"]
        }
        channel_details = pd.DataFrame(channel_data, index=[0])

        return channel_details

    #to fetch multiple playlist ids in a channel


    def all_playlist(channel_id):
        playlist = []
        next_playlist_pagetoken = None
        while True:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_playlist_pagetoken
            )

            playlist_response = request.execute()

            for item in playlist_response['items']:
                data = {
                    'PlaylistId': item['id'],
                    'Playlist_name': item['snippet']['title'],
                    'ChannelId': item['snippet']['channelId'],
                    'VideoCount': item['contentDetails']['itemCount'],
                }
                playlist.append(item['id'])
            next_playlist_pagetoken = playlist_response.get('nextpageToken')

            if not next_playlist_pagetoken:
                break

        return playlist


    # fetch video ids in the playlist


    def video_id(playlist):
        video_ids = []
        next_playlist_pagetoken = None

        while True:
            for playID in playlist:
                request = youtube.playlistItems().list(
                    part="snippet, contentDetails",
                    playlistId=playID,
                    maxResults=50,
                    pageToken=next_playlist_pagetoken
                )
                playlist_items_response = request.execute()
                for i in playlist_items_response['items']:
                    vi_id = i['contentDetails']['videoId']
                    video_ids.append(vi_id)

                next_playlist_pagetoken = playlist_items_response.get('nextpageToken')

            if not next_playlist_pagetoken:
                break

        return video_ids


    def video_details(video_ids):
        final = []
        videoid=[]
        for vi_id in video_ids:
            video_request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=vi_id,
                maxResults=50
            )
            vi_response = video_request.execute()

            try:
                if vi_id in videoid:
                    pass
                elif vi_response['items'][0]['snippet']['title'] == "Private video":
                    pass
                else:
                    for i in vi_response['items']:
                        data = {"channel_id":i['snippet']['channelId'],
                                "video_id":i['id'],
                                "video_name":i['snippet']['title'],
                                "video_description":i['snippet']['description'],
                                "video_published_at":i['snippet']['publishedAt'],
                                "view_count":i['statistics']['viewCount'],
                                "like_count":i['statistics'].get('likeCount'),
                                "dislike_count":i.get('dislikeCount'),
                                "favorite_count":i['statistics']['favoriteCount'],
                                "comment_count":i['statistics'].get('commentCount'),
                                "duration":i['contentDetails']['duration'],
                                "thumbnail":i['snippet']['thumbnails']['default'] ['url'],
                                "caption_status":i['contentDetails']['caption'],
                                "tags":",".join(i['snippet'].get('tags',["na"]))}

                    # print(data)
                    videoid.append(vi_id)
                    final.append(data)

            except IndexError:
                pass

        video_detail = pd.DataFrame(final)
        return video_detail


    def comment_details(video):
        comment_details = []

        try:
            for video_id in video:
                comment_request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100)

                response = comment_request.execute()

                for i in response['items']:
                    comment_data = {
                                 "comment_id": i['snippet']['topLevelComment']['id'],
                                 "video_id": i['snippet']['videoId'],
                                 "comment_text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                 "comment_author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                 "comment_published_at": i['snippet']['topLevelComment']['snippet']['publishedAt']}
                    comment_details.append(comment_data)
        except:
            pass

        comments = pd.DataFrame(comment_details)
        return comments


    channel_details = channel(youtube_channel_id)
    playlists = all_playlist(youtube_channel_id)
    video_ids = video_id(playlists)
    videos_details = video_details(video_ids)
    com = comment_details(video_ids)



    # connection to mysql database


    db = pymysql.connect(
        host="localhost",
        user="root",
        password="1234"

    )

    mysql = "mysql+pymysql://root:1234@localhost/youtube"
    engine = create_engine(mysql)

    connect = db.cursor()

    def channel_table():

        connect.execute('create database if not exists youtube')
        connect.execute('use youtube')

        # connect.execute('drop table if exists comment')
        # connect.execute('drop table if exists video')
        # connect.execute('drop table if exists channel')
        connect.execute('''create table if not exists channel( channel_id VARCHAR(50) primary key,
                                        channel_name VARCHAR(1000),description VARCHAR(1000),
                                        upload_id VARCHAR(50), views_count BIGINT,
                                        sub_count BIGINT, video_count BIGINT)''')

        db.commit()
        channel_details.to_sql('channel',engine,if_exists='append',index=False)



    def video_table():
        connect.execute('create database if not exists youtube')
        connect.execute('use youtube')


        connect.execute('''create table if not exists video( channel_id VARCHAR(50),video_id VARCHAR(50) PRIMARY KEY ,
                                        video_name VARCHAR(1000) ,video_description VARCHAR(10000),video_published_at VARCHAR(50),view_count BIGINT,
                                        like_count BIGINT,dislike_count BIGINT,favorite_count BIGINT,comment_count BIGINT,
                                        duration VARCHAR(10),thumbnail VARCHAR(100),caption_status VARCHAR(50),tags VARCHAR(1000),FOREIGN KEY (channel_id) REFERENCES channel(channel_id))''')


        db.commit()
        videos_details.to_sql('video',engine, if_exists='append',index=False)


    # comment table
    def comment_table():
        connect.execute('create database if not exists youtube')
        connect.execute('use youtube')
        connect.execute('''create table if not exists Comment( video_id VARCHAR(50), comment_id VARCHAR(50),
                                       comment_text TEXT,comment_author VARCHAR(1000),comment_published_at VARCHAR(100), FOREIGN KEY (video_id) REFERENCES video(video_id))''')
        db.commit()
        com.to_sql('comment', engine, if_exists='append', index=False)

        engine.dispose()

    channel_table()
    video_table()
    comment_table()



st.title(":red[Youtube] [Data Harvesting & Warehousing using SQL]")
st.subheader(':blue[Domain :] Social Media')


with st.sidebar:


    youtube_channel_id=st.text_input("**Enter the channel ID in the below box:**")
    if st.button("Extract"):
        fetch_details()
        st.success('**Successfully uploaded in MySQL Database**')



tab1, tab2 = st.tabs(["View Channels", "Queries"])

with tab1:
    Check_channel = st.checkbox('**Show the Channel Details**')
    if Check_channel:
        my_connection = pymysql.connect(host="127.0.0.1",user="root",password="1234")
        mycursor = my_connection.cursor()
        mycursor.execute("select channel_name,channel_id from youtube.channel")
        channel_df= pd.DataFrame(mycursor.fetchall(),columns=['Channel_name','Channel_Id'])
        channel_df.drop_duplicates(inplace=True)
        channel_df.index += 1
        st.write(channel_df)


with tab2:
    st.write("Select SQL Queries to show the output")

    SQL_Queries = st.selectbox("SQL Queries",
                               ("Select Your Questions",
                                "1. What are the names of all the videos and their corresponding channels?",
                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                "3. What are the top 10 most viewed videos and their respective channels?",
                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"),
                               key="collection_question")

    # Query 1

    my_connection = pymysql.connect(host="127.0.0.1", user="root", password="1234")
    mycursor = my_connection.cursor()
    mycursor.execute('use youtube')

    if SQL_Queries == "1. What are the names of all the videos and their corresponding channels?":
        mycursor.execute(
            "select c.channel_name,v.video_name from video as v inner join channel as c on v.channel_id = c.channel_id")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_name'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "2. Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("select channel_name,video_count from channel order by video_count desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_count'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "3. What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute(
            "select c.channel_name,v.video_name,v.view_count from video as v inner join channel as c on v.channel_id = c.channel_id order by view_count desc limit 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_name', 'Most_viewed_video'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("select video_name,comment_count from video order by comment_count desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Comment_count'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute(
            "select c.channel_name,sum(v.like_count) as Highest_likes from video as v inner join channel as c on v.channel_id=c.channel_id group by c.channel_name order by Highest_likes desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_Likes'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("select video_name,like_count from video order by like_count desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Like_count'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("select channel_name,views_count from channel order by views_count desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'View_count'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute(
            "select distinct channel_name from channel as c inner join video as v on v.channel_id=c.channel_id where video_published_at between '2022-01-01 00:00:00' and '2022-12-31 23:59:59'")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute(
            "select c.channel_name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v.duration)))), '%H:%i:%s') as average_duration from video as v inner join channel as c on v.channel_id=c.channel_id group by c.channel_name")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Average_duration'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute(
            "select c.channel_name,sum(v.comment_count) as Highest_comment from video as v inner join channel as c on v.channel_id=c.channel_id group by c.channel_name order by Highest_comment desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_comment'])
        df.index += 1
        st.write(df)





st.subheader(':blue[Overview :]')
st.markdown('''In this project, I built an simple UI using Streamlit,
            retrieved data from YouTube API and stored it in a SQL database as a data warehouse,
            and queried the data warehouse with SQL.
            then the processed data will displayed dynamically within the Streamlit app''')
st.subheader(':blue[Skill Take Away :]')
st.markdown(''' Python scripting, Data Collection, API integration, Data Management using SQL and Streamlit''')
