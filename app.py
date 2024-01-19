#Libraries to import
import streamlit as st
from googleapiclient.discovery import build
import re
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import pymysql
import pandas as pd
from datetime import datetime
import json
from streamlit_lottie import st_lottie 
import numpy as np
import plotly.express as px
from streamlit_option_menu import option_menu


st.set_page_config(page_title= "Youtube Data Analysis",
                   page_icon= '📈',
                   layout= "wide",)

# Access youtube API
def Api_connect():
    Api_Id="AIzaSyAWrJl51h0n-3YP90fkZSkpFpbQAVOCFuc"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

#function to convert duration
def duration_to_seconds(duration):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
    if match:
        hours = int(match.group(1)[:-1]) if match.group(1) else 0
        minutes = int(match.group(2)[:-1]) if match.group(2) else 0
        seconds = int(match.group(3)[:-1]) if match.group(3) else 0
        return hours * 3600 + minutes * 60 + seconds
    return 0



#  function to retrieve channel data
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    

 #  function to  retrive playlist  data
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,                    #as mentioned by google max is 50 and default is 5
            pageToken=next_page_token
            )
        response = request.execute()
        
        for item in response['items']: 
            published_dates = item["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates, '%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d').replace('T', '').replace('Z', '')
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':format_date,
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data   


def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


        #  function to retrieve video details
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()
        


        for item in response["items"]:
            published_dates = item["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates, '%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d').replace('T', '').replace('Z', '')

            duration = item['contentDetails']['duration']
            duration_seconds = duration_to_seconds(duration)
    
            format_time = str(timedelta(seconds=duration_seconds))
            format_time = format_time[2:] 
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = format_date,
                        Duration = format_time,
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


        #  function to retrieve comment data
def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                parsed_comment_published = datetime.strptime(comment_published, '%Y-%m-%dT%H:%M:%SZ')
                                format_comment_published = parsed_comment_published.strftime('%Y-%m-%d %H:%M:%S').replace('T', ' ').replace('Z', '')
                                
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = format_comment_published)

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information

# -----------------------------------    /   MongoDB connection and store the collected data   /    ---------------------------------- #

client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb")


db = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"

# ========================================   /     Data Migrate zone (Stored data to MySQL)    /   ========================================== #
#connect to mysql
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345678',
    database = 'youtube_db'
    
)
cursor = connection.cursor()

def channels_table():
    connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )

    
    cursor = connection.cursor()
    try:
        create_query = '''create table IF NOT EXISTS channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        connection.commit()
    except:
        print("Channels Table already created")    
    

ch_list = []
db = client["Youtube_data"]
coll1 = db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    ch_list.append(ch_data["channel_information"])
df = pd.DataFrame(ch_list)


for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            

        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            connection.commit()    
        except:
            print("channel tables values inserted")  


def playlists_table():
     connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )

     cursor = connection.cursor()
     try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(500), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt Timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        connection.commit()
     except:
        print("playlist table created") 


db = client["Youtube_data"]
coll1 =db["channel_details"]
pl_list = []
for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
    for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
df1 = pd.DataFrame(pl_list)     



connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )

cursor = connection.cursor()
for index,row in df1.iterrows():
    insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
    values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
    try:                     
            cursor.execute(insert_query,values)
            connection.commit()    
    except:
            print("playlist values are inserted")



connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )

cursor = connection.cursor()


def videos_table():
     connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )

     cursor = connection.cursor()
     
     create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, Title varchar(150), 
                        Tags text,Thumbnail varchar(225),
                        Description text, Published_Date timestamp,
                        Duration time , Views bigint, 
                        Likes bigint,Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )''' 
                        
     cursor.execute(create_query)             
     connection.commit()
     
    


vi_list = []

db = client["Youtube_data"]
coll1 = db["channel_details"]
for vi_data in coll1.find({},{"_id":0,"video_information":1}):
    for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
df2 = pd.DataFrame(vi_list)


for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                        Channel_Id,Video_Id, Title, 
                        Tags,Thumbnail,Description, 
                        Published_Date,Duration, 
                        Views,Likes,Comments,
                        Favorite_Count,Definition, 
                        Caption_Status 
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = (
                    row['Channel_Name'],row['Channel_Id'],
                    row['Video_Id'],row['Title'],
                    row['Tags'],row['Thumbnail'],
                    row['Description'],row['Published_Date'],
                    row['Duration'],row['Views'],
                    row['Likes'],row['Comments'],
                    row['Favorite_Count'],row['Definition'],
                    row['Caption_Status'])
                                
        try:    
            cursor.execute(insert_query,values)
            connection.commit()
        except:
            print("video values already inserted in the table")


def comments_table():
    connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )
    cursor = connection.cursor()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        connection.commit()
        
    except:
        print("Comments Table already created")


com_list = []
db = client["Youtube_data"]
coll1 = db["channel_details"]
for com_data in coll1.find({},{"_id":0,"comment_information":1}):
    for i in range(len(com_data["comment_information"])):
        com_list.append(com_data["comment_information"][i])
df3 = pd.DataFrame(com_list)




for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id, Video_Id ,
                                      Comment_Text,Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)
            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            
            try:
               cursor.execute(insert_query,values)
               connection.commit()
            except:
                print("Comments Exist")

            
               



def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table
    

def show_playlists_table():
    db = client["Youtube_data"]
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table


def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table






st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

with st.sidebar:
    selected=option_menu(
        menu_title="Main Menu",
        options=["About","Data Collection","Data Analysis"],
    )

if selected=="About":
    st.title(":black[About the Project]")
    

    path = "J:/visual_studio_codes/ani.json"
    with open(path,"r") as file: 
      url = json.load(file) 
  
    st_lottie(url, 
       reverse=True, 
       height=200, 
       width=200, 
       speed=1, 
       loop=True, 
       quality='low', 
       key=None
    
)
    st.subheader("Hi All,I am Janani :wave:")
    st.write ("   This project seamlessly integrates **:red[Google API]** for robust data extraction, employs  **:red[MongoDB]** data lake for flexible storage, and features a streamlined button-triggered workflow for simultaneous **data collection from multiple YouTube channels**. ")
    st.write("    Furthermore, it showcases adaptability by facilitating the migration of data from MongoDB to a **:red[MYSQL database]**. ")
    st.write("    The interactive **:red[Streamlit interface]** enhances user experience, while advanced SQL querying capabilities empower users to perform detailed searches,including table joins for comprehensive insights. ")
    st.write("    This project encompasses a diverse skill set, ranging from API integration to NoSQL and relational database management, emphasizing a holistic approach to data analysis and application development.")
if selected=="Data Collection":
     st.write ('(Note:- This zone **collect data** by using channel id and **stored it in the :green[MongoDB] database**.)')
     channel_id = st.text_input("Enter the Channel id")
     channels = channel_id.split(',')
     channels = [ch.strip() for ch in channels if ch]

     if st.button("Collect and Store data"):
      for channel in channels:
        ch_ids = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
      if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)
         
     st.write ('(Note:- This zone previews different tables stored in :green[MYSql] database.)')
     table_view = st.selectbox(

     "Select table to view",
         ("Select Here",
          "Channels Table",
          "Playlist Table",
         "Videos Table",
         "Comments Table") ,key="tables")
    
     if table_view == 'Channels Table':
      
      st.write("Here is **Channels Table**")
      show_channels_table()

     elif table_view == ':red[Playlist Table]':
      
      st.write("Here is **PlayList Table**")
      show_playlists_table()

     elif table_view == 'Videos Table':
      st.write("Here is **Videos Table**")
      show_videos_table()

     elif table_view == ':red[Comments Table]':
      st.write("Here is **Comments Table**")
      show_comments_table()
     
    
if selected=="Data Analysis":

    st.title(f"you have selected {selected}")
    question = st.selectbox(
      'Please Select Your Question',

    ("-----------------------------Select Here-----------------------------------",
     '1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
      '7. What is the total number of views for each channel, and what are their corresponding channel names?',
      '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')

    connection = pymysql.connect(
            host="localhost",
            user="root",
            password="12345678",
            database = 'youtube_db',
            port = 3306
            )
    cursor = connection.cursor()

    if question == '1. What are the names of all the videos and their corresponding channels?':
       cursor.execute("SELECT Channel_Name, Title as video_name FROM videos;")
       result_1 = cursor.fetchall()
       dfa= pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
       dfa.index += 1
       st.dataframe(dfa)

    elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
       col1,col2 = st.columns(2)
       with col1:
           cursor.execute("SELECT Channel_Name, Total_Videos FROM channels ORDER BY Total_Videos DESC;")
           result_2 = cursor.fetchall()
           dfb = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
           dfb.index += 1
           st.dataframe(dfb)
       with col2:
           fig_vc = px.bar(dfb, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
           fig_vc.update_traces(textfont_size=16,marker_color='#E6064A')
           fig_vc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
           st.plotly_chart(fig_vc,use_container_width=True)
        
    elif question == '3. What are the top 10 most viewed videos and their respective channels?':

      col1,col2 = st.columns(2)
      with col1:
        cursor.execute ("select Channel_Name as ChannelName , Title as VideoTitle,Views as views from videos where Views is not null order by Views desc limit 10;")

        result_3 = cursor.fetchall()
        dfc = pd.DataFrame(result_3,columns=['Channel Name', 'Video Title', 'View count']).reset_index(drop=True)
        dfc.index += 1
        st.dataframe(dfc)

      with col2:
        fig_topvc = px.bar(dfc, y='View count', x='Video Title', text_auto='.2s', title="Top 10 most viewed videos")
        fig_topvc.update_traces(textfont_size=16,marker_color='#E6064A')
        fig_topvc.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
        st.plotly_chart(fig_topvc,use_container_width=True)

    elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
       cursor.execute("select Title as VideoTitle ,Comments as No_comments  from videos where Comments is not null;")
       result_4 = cursor.fetchall()
       dfd = pd.DataFrame(result_4,columns=[ 'Video Name', 'Comment count']).reset_index(drop=True)
       dfd.index += 1
       st.dataframe(dfd)

    elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
       cursor.execute("select Channel_Name as ChannelName,Title as VideoTitle, Likes as LikesCount from videos where Likes is not null order by Likes desc;")
       result_5= cursor.fetchall()
       df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
       df5.index += 1
       st.dataframe(df5)


    elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
       cursor.execute("select Title as VideoTitle, Likes as likeCount from videos;")
       result_6= cursor.fetchall()
       df6 = pd.DataFrame(result_6,columns=[ 'Video Name', 'Like count',]).reset_index(drop=True)
       df6.index += 1
       st.dataframe(df6)

    elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':

        col1, col2 = st.columns(2)
        with col1:
           cursor.execute("SELECT Channel_Name, Views FROM channels ORDER BY Views DESC;")
           result_7= cursor.fetchall()
           df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
           df7.index += 1
           st.dataframe(df7)

        with col2:
           fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
           fig_topview.update_traces(textfont_size=16,marker_color='#E6064A')
           fig_topview.update_layout(title_font_color='#1308C2 ',title_font=dict(size=25))
           st.plotly_chart(fig_topview,use_container_width=True)

    elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT Channel_Name as ChannelName,Title as Video_Title, DATE_FORMAT(Published_Date, '%Y-%m-%d') as VideoRelease FROM videos WHERE EXTRACT(YEAR FROM Published_Date) = 2022;")

        result_8= cursor.fetchall()
        df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Date Published']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)

    elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':

        cursor.execute("SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;")
        result_9= cursor.fetchall()
        df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        for index, row in df9.iterrows():
           channel_title = row['Channel Name']
           average_duration = row['Average duration of videos (HH:MM:SS)']
           average_duration_str = str(average_duration)
           df9.append({"Channel Title": channel_title , "Average Duration": 'Average duration of videos (HH:MM:SS)'})
           st.dataFrame(df9)


    elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("select Channel_Name as ChannelName,Title as VideoTitle,  Comments as Comments from videos where Comments is not null order by Comments desc;")
        result_10= cursor.fetchall()
        df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)



