#!/usr/bin/env python
# coding: utf-8

# In[3]:


from googleapiclient.discovery import build
import googleapiclient.discovery
import pandas as pd
import mysql.connector
from datetime import datetime
import re
import streamlit as st

#API Extraction
def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="api_key"
    youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key)
    return youtube

#storing the function another variable
youtube=api_connect() 

# to get Channel Id
def get_channel_data(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics,status",
    id=channel_id
    )
    response = request.execute()
    channel_Data=[]
    if 'items' in response:
        for item in response['items']:
            data={
                "channel_id": item['id'],
                "channel_name": item['snippet']['title'],
                "channel_description": item['snippet']['description'],
                "channel_playlistid":item['contentDetails']['relatedPlaylists']['uploads'],
                "channel_subscribers":item['statistics']['subscriberCount'],
                "channel_view":item['statistics']['viewCount'],
                "channel_video":item['statistics']['videoCount'],
                "channel_status":item['status']['privacyStatus'] 
            }
        channel_Data.append(data)
    return channel_Data

# get Video ID's
def get_video_Id_data(channel_id):
    video_ids=[]
    try:
        response= youtube.channels().list(part='contentDetails', id=channel_id).execute()
        if 'items' in response:
            channel_playlistid=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            next_page_token=None
            while True:   
                response1 = youtube.playlistItems().list( part='snippet',
                                                         playlistId=channel_playlistid,
                                                         maxResults=50,
                                                         pageToken=next_page_token).execute()
                for i in response1.get('items',[]):
                    video_ids.append(i['snippet']['resourceId']['videoId'])
                next_page_token= response1.get('nextPageToken') 
                if not next_page_token:
                    break
    except KeyError as e:
        print(f"KeyError: {e}. Response may not contain 'items' key.")
    except Exception as e:
        print(f"An error has occurred: {e}")
    return video_ids

# get_video_Details
def get_video_data(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()
        for item in response['items']:
            data={
                    'channel_name':item['snippet']['channelTitle'],
                    'Channel_ID':item['snippet']['channelId'],
                    'Video_ID':item['id'],
                    'Video_Title':item['snippet']['title'],
                    'Video_Thumbnail':item['snippet']['thumbnails']['default']['url'],
                    'Description':item['snippet']['description'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'Duration':item['contentDetails']['duration'],
                    'Video_views':item['statistics'].get('viewCount'),
                    'Favorite_Count':item['statistics'].get('favoriteCount'),
                    'Like_Count':item['statistics'].get('likeCount'),
                    'Dislike_Count':item['statistics'].get('dislikeCount'),
                    'Video_comment':item['statistics'].get('commentCount'),
                    'Caption_Status':item['contentDetails']['caption'],
                    'Tags':item['snippet'].get('tags')
                }         
            video_data.append(data)
    return video_data

# get comment details
def get_comment_data(video_ids):
    Comment_data=[]
    try: 
        for video_id in video_ids:
            request=youtube.commentThreads().list(part="snippet",
                                                videoId=video_id,
                                                maxResults=50)
            response=request.execute()
            for item in response['items']:
                data={'Comment_id':item['snippet']['topLevelComment']['id'],
                        'Video_Id':item['snippet']['topLevelComment']['snippet']['videoId'],
                        'Comment_text':item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_Author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_PublishedAt':item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# get Playlist details
def get_playlist_data(channel_id):
    next_page_token = None
    all_data = []
    try:
        if channel_id:
            while True:
                request = youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token)
                response = request.execute()
                for item in response.get('items', []):
                    data = {
                        'Playlist_Id': item['id'],
                        'Channel_id': item['snippet']['channelId'],
                        'Playlist_Name': item['snippet']['title'],
                        'Channel_Name': item['snippet']['channelTitle'],
                        'Video_Count': item['contentDetails']['itemCount']
                    }
                    all_data.append(data)
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
        else:
            print("No channel ID has been provided")
    except Exception as e:
        print(f"An error occurred while fetching the playlist data: {e}")
    return all_data

# STREAMLIT APP
st.title(":green[DataNest: Unlocking YouTube's Insights]")
st.write("Glad that you are here to unveil YouTube's Insights. Decode trends, understand audience behavior, and maximize impact effortlessly. Your key to informed decision-making")

# Get user input for the YouTube Channel ID
channel_ids=set()
def get_channel_id():
    return st.text_input("# :green[Enter a YouTube Channel ID:]")
channel_id=get_channel_id()
st.button("Move to Database")

# Initialize YouTube API and get data
channel_info=get_channel_data(channel_id)
videoid_info=get_video_Id_data(channel_id)
video_info=get_video_data(videoid_info)
comment_info=get_comment_data(videoid_info)
playlist_info=get_playlist_data(channel_id)

# Convert data to DataFrames
channel=pd.DataFrame(channel_info)
videos=pd.DataFrame(video_info)
Comments=pd.DataFrame(comment_info)
playlist=pd.DataFrame(playlist_info)

# Display the collected data
st.subheader('Channels')
st.dataframe(channel)

st.subheader('Videos')
st.dataframe(videos)

st.subheader('Commments')
st.dataframe(Comments)

st.subheader('Playlists')
st.dataframe(playlist)

#SQL connection
mydb = mysql.connector.connect(host="localhost",user="root",password="Abisheg@7103")
print(mydb)
mycursor = mydb.cursor(buffered=True)
# mycursor.execute('create database youtubeproject')
mycursor.execute('use youtube_data')

#Channel- table creation for sql database
def channel_table():
    try:
        #Create table in SQL
        query = '''create table if not exists channel_data (channel_id varchar(255),
                                                        channel_name varchar(255),
                                                        channel_subscribers varchar(255),
                                                        channel_view int,
                                                        channel_description text,
                                                        channel_playlistid varchar(255),
                                                        channel_status varchar(255),
                                                        channel_video int)'''
        mycursor.execute(query)
        mydb.commit()
    except Exception as e:
        print('Creating channel table failed:', e)
channel_table()

# Inserting Channel details
if channel_id:
    channel_ids.add(channel_id)
    st.success(f"Channel ID '{channel_id}' added to migration list.")
    channel_info = get_channel_data(channel_id)

    Insert_Query = '''INSERT INTO channel_data(channel_id,channel_name,channel_subscribers,channel_view,channel_description,channel_playlistid,channel_status,channel_video) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

    try:
        for channel_infos in channel_info:
        # Check if the channel_id already exists in the table
            query = 'SELECT * FROM channel_data WHERE channel_id = %s'
            channel_id_tuple = (channel_infos ['channel_id'],)
            mycursor.execute(query, channel_id_tuple)
            existing_channel = mycursor.fetchone()
            if not existing_channel:
                val = (
                    channel_infos['channel_id'],
                    channel_infos['channel_name'],
                    channel_infos['channel_subscribers'],
                    channel_infos['channel_view'],
                    channel_infos['channel_description'],
                    channel_infos['channel_playlistid'],
                    channel_infos['channel_status'],
                    channel_infos['channel_video'])
                mycursor.execute(Insert_Query, val)
                mydb.commit()
                print(f"Channel details {channel_infos['channel_name']} inserted successfully!")
            else:
                print(f"Channel details for {channel_infos['channel_name']} already exist,Skipping insertion.")
    except KeyError:
        print("Error: 'channel_id' key missing in channel_info")
    except mysql.connector.Error as err:
        print("Error:", err)
else:
    st.warning("Please enter a valid Channel ID before migrating to SQL.")

# Video table creation for video in SQL
def video_table():
    try:
        query = '''create table if not exists videos_data(channel_name varchar(255),
                                                            Video_ID varchar(255),
                                                            Video_Title varchar(255),
                                                            Video_Thumbnail varchar(255),
                                                            Description text,
                                                            PublishedAt datetime,
                                                            Duration int(255),
                                                            Video_views int,
                                                            Favorite_Count int(255),
                                                            Like_Count int,
                                                            Dislike_Count int,
                                                            Video_comment varchar(255),
                                                            Caption_Status varchar(255),
                                                            Tags varchar(255))'''
        mycursor.execute(query)
        mydb.commit()
    except:
        print("Created Video Table")
video_table()

# Insert video Details Video_ID
try:
    for video_data in video_info:
            sql = """INSERT INTO videos_data(channel_name, Video_ID, Video_Title, Video_Thumbnail, Description, PublishedAt,
                                                Duration, Video_views, Like_Count, Dislike_Count, Favorite_Count, 
                                                Video_comment, Caption_Status, Tags) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            duration = video_data['Duration']
            match = re.match(r'^PT(\d+H)?(\d+M)?(\d+S)?$', duration)
            if match:
                hours = int(match.group(1)[:-1]) if match.group(1) else 0
                minutes = int(match.group(2)[:-1]) if match.group(2) else 0
                seconds = int(match.group(3)[:-1]) if match.group(3) else 0
                duration_formatted = hours * 3600 + minutes * 60 + seconds
            else:
                duration_formatted = None

            PublishedAt = datetime.strptime(video_data['PublishedAt'], "%Y-%m-%dT%H:%M:%SZ")
            tags = ', '.join(video_data.get("Tags", [])) if video_data.get("Tags") else None

            val = (video_data['channel_name'], video_data['Video_ID'], video_data['Video_Title'],
                    video_data['Video_Thumbnail'], video_data['Description'], PublishedAt, duration_formatted,
                    video_data['Video_views'], video_data['Like_Count'], video_data['Dislike_Count'],
                    video_data['Favorite_Count'], video_data['Video_comment'], video_data['Caption_Status'], tags)
            mycursor.execute(sql, val)
    mydb.commit()
    print("All video details inserted successfully!")
except Exception as e:
    print("Error inserting records:", e)

# Playlist table creation for playlist details
def playlist_table():
    try:
        query = '''create table if not exists playlist_data(Playlist_Id varchar(255),
                                                                Channel_id varchar(255),
                                                                Playlist_Name varchar(255),
                                                                Channel_Name varchar(255),
                                                                Video_Count int)'''
        mycursor.execute(query)
        mydb.commit()
    except Exception as e:
        print('Creating playlist table failed:', e)
playlist_table()

#Inserting values to playlist table
try:
    for playlist_detail in playlist_info:
        # Check if the playlist already exists in the table based on Playlist_Id
        query = "SELECT COUNT(*) FROM playlist_data WHERE Playlist_Id = %s"
        mycursor.execute(query, (playlist_detail["Playlist_Id"],))
        result = mycursor.fetchone()
        if result[0] == 0:  # If the playlist does not exist, insert it
            sql = '''INSERT INTO playlist_data(Playlist_Id,Channel_id,Playlist_Name,Channel_Name,Video_Count) VALUES (%s,%s,%s,%s,%s)'''
            val =(
                playlist_detail["Playlist_Id"],
                playlist_detail["Channel_id"],
                playlist_detail["Playlist_Name"],
                playlist_detail["Channel_Name"],
                playlist_detail["Video_Count"]
                )
            mycursor.execute(sql, val)
            mydb.commit()
            print("Playlist inserted successfully!")
        else:
            print("Playlist already exists, skipping insertion.")
except Exception as e:
    print("Error inserting playlists:", e)

#Creating Comment table
def comments_table():
    try:
        query = '''create table if not exists comments_data(Comment_id varchar(255),
                                                                Video_Id varchar(255),
                                                                Comment_text text,
                                                                Comment_Author varchar(255),
                                                                Comment_PublishedAt datetime)'''
        mycursor.execute(query)
        mydb.commit()
    except:
        print("Creating channel table")
comments_table()

# Inserting values Comment
def check_duplicate(comment_id):
    query = "SELECT COUNT(*) FROM comments_data WHERE Comment_id = %s"
    mycursor.execute(query, (comment_id,))
    result = mycursor.fetchone()
    return result[0] > 0

for comment_data in comment_info:
    if not check_duplicate(comment_data['Comment_id']):
        Comment_PublishedAt = datetime.strptime(comment_data['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        val = (comment_data['Comment_id'], comment_data['Video_Id'], comment_data['Comment_text'],
                comment_data['Comment_Author'], Comment_PublishedAt)
        try:
            sql = '''INSERT INTO comments_data (Comment_id, Video_Id, Comment_text, Comment_Author, Comment_PublishedAt)
                    VALUES (%s, %s, %s, %s, %s)'''
            mycursor.execute(sql, val)
            mydb.commit()
            print("Comments are inserted successfully!")
        except mysql.connector.Error as err:
            print("Error:", err)
    else:
        print("Duplicate comment found and skipped:", comment_data['Comment_id'])
        
# Fetching data from tables
def fetch_channel_db():
    mycursor.execute("SELECT * FROM channel_data")
    data = mycursor.fetchall()
    df1 = pd.DataFrame(data, columns=['channel_id','channel_name','channel_subscribers','channel_view','channel_description',
                                    'channel_playlistid','channel_status','channel_video'])
    df1.drop_duplicates(subset=["channel_id"], inplace=True)
    return df1


def fetch_playlist_db():
    mycursor.execute("SELECT * FROM playlist_data")
    data = mycursor.fetchall()
    df2 = pd.DataFrame(data, columns=['Playlist_Id','Channel_id','Playlist_Name','Channel_Name','Video_Count'])
    df2.drop_duplicates(subset=["Playlist_Id"], inplace=True)
    return df2

def fetch_video_db():
    mycursor.execute("SELECT * FROM videos_data")
    data = mycursor.fetchall()
    df3 = pd.DataFrame(data, columns=['channel_name','Video_ID','Video_Title','Video_Thumbnail','Description', 
                                    'PublishedAt', 'Duration','Video_views', 'Like_Count','Dislike_Count', 
                                    'Favorite_Count','Video_comment','Caption_Status','Tags'])
    df3.drop_duplicates(subset=["Video_ID"], inplace=True)
    return df3

def fetch_comment_db():
    mycursor.execute("SELECT * FROM comments_data")
    data = mycursor.fetchall()
    df4 = pd.DataFrame(data, columns=['Comment_id', 'Video_Id', 'Comment_text', 'Comment_Author', 'Comment_PublishedAt'])
    df4.drop_duplicates(subset=["Comment_id"], inplace=True)
    return df4

# Streamlit main function
def main():
    show_table = st.radio("# :green[Select the table for View:-]", ("CHANNEL", "PLAYLIST", "VIDEOS", "COMMENTS"))
    if show_table == "CHANNEL":
        df1=fetch_channel_db()
        st.dataframe(df1)
    elif show_table == "PLAYLIST":
        df2=fetch_playlist_db()
        st.dataframe(df2)
    elif show_table == "VIDEOS":
        df3=fetch_video_db()
        st.dataframe(df3)
    elif show_table == "COMMENTS":
        df4=fetch_comment_db()
        st.dataframe(df4)

    question = st.selectbox("Select your question", [
        "1. Name of all the videos and corresponding Channels",
        "2. Channels having most number of videos and their video count",
        "3. Top 10 most viewed videos and their respective channels",
        "4. Name of each video and the comments count",
        "5. Videos with highest likes and their corresponding Channel names",
        "6. Total number of likes and dislikes for each video and their corresponding video names",
        "7. Total number of views for each channel, and their corresponding channel names",
        "8. Names of all the channels that have published videos in the year 2022",
        "9. Videos having the highest number of comments, and their corresponding channel names",
        "10. Average duration of all videos in each channel, and their corresponding channel names"
    ])

    # SQL query based on selected question
    # --- 1 ---
    if question =="1. Name of all the videos and corresponding Channels":
        query1 = '''SELECT channel_name, Video_Title FROM videos_data'''
        mycursor.execute(query1)
        q1 = mycursor.fetchall()
        df1 = pd.DataFrame(q1, columns=["channel_name", "Video_Title"])
        st.dataframe(df1, width=1000)

    #---2---
    elif question == "2. Channels having most number of videos and their video count":
        query2 = '''SELECT channel_name, channel_video FROM channel_data ORDER BY channel_video DESC'''
        mycursor.execute(query2)
        q2 = mycursor.fetchall()
        df2 = pd.DataFrame(q2, columns=["channel_name", "channel_video"])
        st.dataframe(df2)

    # Add the rest of the questions 3 to 10 in a similar manner...

    #----3----
    elif question == "3. Top 10 most viewed videos and their respective channels":
        query3 = '''SELECT channel_name, Video_Title, Video_views FROM videos_data WHERE Video_views IS NOT NULL ORDER BY Video_views DESC LIMIT 10'''
        mycursor.execute(query3)
        q3 = mycursor.fetchall()
        df3 = pd.DataFrame(q3, columns=["channel_name", "Video_Title", "Video_views"])
        st.dataframe(df3, width=1000)

    #----4----
    elif question == "4. Name of each video and the comments count":
        query4 = '''SELECT Video_Title, Video_comment FROM videos_data WHERE Video_comment IS NOT NULL'''
        mycursor.execute(query4)
        q4 = mycursor.fetchall()
        df4 = pd.DataFrame(q4, columns=["Video_Title", "Video_comment"])
        st.dataframe(df4, width=1000)

    #----5----
    elif question == "5. Videos with highest likes and their corresponding Channel names":
        query5 = '''SELECT channel_name, Video_Title, Like_Count 
                    FROM videos_data 
                    WHERE Like_Count IS NOT NULL ORDER BY Like_Count DESC'''
        mycursor.execute(query5)
        q5 = mycursor.fetchall()
        df5 = pd.DataFrame(q5, columns=["channel_name", "Video_Title", "Like_Count"])
        st.dataframe(df5, width=1000)

    #----6----
    elif question == "6. Total number of likes and dislikes for each video and their corresponding video names":
        query6 = '''SELECT Video_Title, Like_Count, Dislike_Count 
                    FROM videos_data'''
        mycursor.execute(query6)
        q6 = mycursor.fetchall()
        df6 = pd.DataFrame(q6, columns=["Video_Title", "Like_Count", "Dislike_Count"])
        st.dataframe(df6, width=1000)

    #----7----
    elif question ==  "7. Total number of views for each channel, and their corresponding channel names":
        query7 = '''SELECT channel_name, SUM(Video_views) AS Total_Views 
                    FROM videos_data 
                    WHERE Video_views IS NOT NULL GROUP BY channel_name'''
        mycursor.execute(query7)
        q7 = mycursor.fetchall()
        df7 = pd.DataFrame(q7, columns=["channel_name", "Total_Views"])
        st.dataframe(df7, width=1000)

    #----8----
    elif question == "8. Names of all the channels that have published videos in the year 2022":
        query8 = '''SELECT DISTINCT channel_name 
                    FROM videos_data
                    WHERE Publish_Date LIKE '%2022%' '''
        mycursor.execute(query8)
        q8 = mycursor.fetchall()
        df8 = pd.DataFrame(q8, columns=["channel_name"])
        st.dataframe(df8, width=1000)

    #----9----
    elif question == "9. Videos having the highest number of comments, and their corresponding channel names":
        query9 = '''SELECT channel_name, Video_Title, Video_comment 
                    FROM videos_data
                    WHERE Video_comment IS NOT NULL ORDER BY Video_comment DESC'''
        mycursor.execute(query9)
        q9 = mycursor.fetchall()
        df9 = pd.DataFrame(q9, columns=["channel_name", "Video_Title", "Video_comment"])
        st.dataframe(df9, width=1000)

    #----10----
    elif question == "10. Average duration of all videos in each channel, and their corresponding channel names":
        query10 = '''SELECT channel_name, AVG(Duration) AS Avg_Duration 
                     FROM videos_data 
                     WHERE Duration IS NOT NULL GROUP BY channel_name'''
        mycursor.execute(query10)
        q10 = mycursor.fetchall()
        df10 = pd.DataFrame(q10, columns=["channel_name", "Avg_Duration"])
        st.dataframe(df10, width=1000)

# Streamlit app starts here
if __name__ == "__main__":
    main()


# In[ ]:




