import streamlit as st
import pandas as pd
import pymysql
from streamlit_option_menu import option_menu
import time
from youtube_oop import Youtube
import numpy as np



def data(channel_id):
    connect = Youtube(channel_id)

    channel= connect.channel()
    videoid= connect.video_ids()
    video_details = connect.video_details(videoid)
    comments= connect.comment_details(videoid)
    
    channel_table = connect.channel_table(channel)
    video_table = connect.video_table(video_details)
    comment_table= connect.comment_table(comments)








with st.sidebar:
    select = option_menu("Menu", ['Home', 'channel details', 'Queries'],icons=["house", "pencil", "bar-chart"],orientation="vertical", styles={
            "container": {"padding": "5px"},
            "icon": {"color": "orange", "font-size": "18px"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px"}})

if select == "Home":
    st.title('Home')
    
    st.markdown("""
    <h1 style='text-align: center; color: blue; font-size: 30px;'>Youtube Data Harvesting & Warehousing using SQL</h1>
    """, unsafe_allow_html=True)
    st.subheader(':red[Domain :] Social Media')

    
    st.markdown(""" Collected Data of Youtube channel through Youtube API, cleaned data into suitable format
                and stored it in pandas dataframe as temporary store before transfering it into the warehouse.
                After collecting data of channel, videos and channels, it stored into SQL tables. Then used SQL queries
                to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input 
                and displayed it on streamlit web application """)
    

    st.markdown('#### Skills learnt in this project')

    st.markdown('#####  1. Python')
    st.markdown('#####  2. API integration')
    st.markdown('#####  3.Data Collection')
    st.markdown('#####  4. Pandas')    
    st.markdown('#####  5. SQL')
    
    st.markdown('#####  6.Streamlit')

elif select == "channel details":
    st.title('channel details')

    channel_id = st.text_input(label= 'Enter a youtube channel id')
    button=st.button(label='fetch')
    if button:
        data(channel_id)

        bar = st.progress(0)
        
        for i in range(1,11):
            bar.progress(i*10)
            time.sleep(0.5)
        st.write('Channel details are collected')

    

    
    
    my_connection = pymysql.connect(host="127.0.0.1",user="root",password="1234", database='youtube')
    mycursor = my_connection.cursor()
    mycursor.execute("select channel_name,channel_id, subscribers, videos from channel")
    channel_df= pd.DataFrame(mycursor.fetchall(),columns=['Channel_name','Channel_Id', 'subscribers', 'videos'])
    channel_df.drop_duplicates(inplace=True)
    channel_df.index += 1
    st.write(channel_df)






    
else:    
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
        mycursor.execute("select channel_name,videos from channel order by videos desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Videos'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "3. What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute(
            "select c.channel_name,v.video_name,v.views from video as v inner join channel as c on v.channel_id = c.channel_id order by views desc limit 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_name', 'Most_viewed_video'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("select video_name,comments from video order by comments desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Comments'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute(
            "select c.channel_name,sum(v.likes) as Highest_likes from video as v inner join channel as c on v.channel_id=c.channel_id group by c.channel_name order by Highest_likes desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_Likes'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("select video_name,likes from video order by likes desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Likes'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("select channel_name,views from channel order by views desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Views'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute(
            "select distinct channel_name from channel as c inner join video as v on v.channel_id=c.channel_id where published_at between '2022-01-01 00:00:00' and '2022-12-31 23:59:59'")
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
            "select c.channel_name,sum(v.comments) as Highest_comment from video as v inner join channel as c on v.channel_id=c.channel_id group by c.channel_name order by Highest_comment desc")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_comment'])
        df.index += 1
        st.write(df)
