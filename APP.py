# Import the necessary Packages
from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime
import re
import streamlit as st


# Function to get the API key connection
def API_Connection():
    API_ID = "AIzaSyD3rCn9yXAOpAgmd6f4WnqKGPQnp-VTTZM"
    API_Service_Name = "YouTube"
    API_Version = "v3"

    youtube = build(API_Service_Name , API_Version , developerKey = API_ID)

    return youtube

youtube = API_Connection()


# To get the channnel details
def Get_Channel_Info(channel_id):

    request = youtube.channels().list(
        part = "snippet , ContentDetails , statistics",
        id = channel_id
    )

    response = request.execute() # here we are executing the request with only one channel id

    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscribers = i["statistics"]["subscriberCount"],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channnel_Description = i["snippet"]["description"],
                    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
    

    # To get video id's
def Get_Video_Ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id = channel_id,
        part = 'contentDetails').execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part = 'snippet',
            playlistId = Playlist_Id ,
            maxResults = 50 ,
            pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# To get Video Information
def Get_Video_Info(video_ids):
    Video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part = "snippet , ContentDetails , statistics",
            id = video_id
        ) 

        response = request.execute()
        for item in response['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_ID = item['snippet']['channelId'],
                        Video_ID = item['id'],
                        Video_Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            Video_data.append(data)
    return Video_data


# To get Comment info
def Get_Comment_Info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_id,
                maxResults = 50,
                )
            response = request.execute()

            for item in response['items']:
                data = dict(
                    comment_Id = item['snippet']['topLevelComment']['id'],
                    Video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_data.append(data)
    except:
        pass
    return Comment_data


# To get the details of Playlist
def Get_Playlist_Info(channel_id):
    Playlist_data = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part = 'snippet , contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(
                Playlist_Id = item['id'],
                Title = item['snippet']['title'],
                Channel_Id = item['snippet']['channelId'],
                Channel_Name = item['snippet']['channelTitle'],
                PublishedAt = item['snippet']['publishedAt'],
                Video_Count = item['contentDetails']['itemCount']
            )
            Playlist_data.append(data)
        
        next_page_token =  response.get('nextPageToken')
        if next_page_token is None : 
            break
    return Playlist_data
    

    # Creating MongoDb Connection 
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["YouTube_Data"]


def Channel_Details(channel_id):
    ch_details = Get_Channel_Info(channel_id)
    pl_details = Get_Playlist_Info(channel_id)
    vi_ids = Get_Video_Ids(channel_id)
    vi_details = Get_Video_Info(vi_ids)
    com_details = Get_Comment_Info(vi_ids)

    collection1 = db["Channel_Details"]
    collection1.insert_one({
        "channel_information" : ch_details , "playlist_information" :  pl_details , "video_information" : vi_details , "comment_information" :  com_details}
        )
    return "upload completed successfully"


# Creating a Channel_Details table and populating it with channel name , channel id , subscribers count , view count , total videos , channel description , playlist id.
def Channels_Table(channels):
    # Creating connection to MySQL DB
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Skramar$13071999",
        database = "YouTube_Data"
    )
    # creating a table in MySql DB to store the channel data
    mycursor = mydb.cursor()

    mycursor.execute('''create table if not exists channels(              
                    Channel_Name varchar(100), 
                    Channel_Id varchar(100) primary key,
                    Subscribers bigint,
                    Views bigint,
                    Total_Videos int,
                    Channnel_Description text,
                    Playlist_Id varchar(100))''')
    mydb.commit()

    # Channel Data extraction from MongoDb and converting it into data frame and inserting the data in MySql DB
    single_channel_detail = []
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    for ch_data in collection1.find({"channel_information.Channel_Name" : channels},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    single_channel_df = pd.DataFrame(single_channel_detail)


    for index , row in single_channel_df.iterrows():
        insert_query = '''insert into channels( Channel_Name , 
                                                Channel_Id , 
                                                Subscribers , 
                                                Views , 
                                                Total_Videos , 
                                                Channnel_Description , 
                                                Playlist_Id )

                                                values(%s , %s , %s , %s , %s , %s , %s)'''

        values = (
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channnel_Description'],
                row['Playlist_Id']
                )
        
        try:
            mycursor.execute(insert_query , values)
            mydb.commit()
        except:
            news = f"The provided channel name {channels} already exist"
            return news


def Playlists_Table(channels):
    # Creating connection to MySQL DB
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Skramar$13071999",
        database = "YouTube_Data"
    )
    # creating a table in MySql DB to store the playlist data
    mycursor = mydb.cursor()
    mycursor.execute(
                '''create table if not exists playlists(              
                        Playlist_Id  varchar(100) primary key, 
                        Title varchar(100),
                        Channel_Id varchar(100),
                        Channel_Name varchar(100),
                        PublishedAt timestamp,
                        Video_Count int
                    )'''
                    )
    mydb.commit()

    # Playlist Data extraction from MongoDb and converting it into data frame and inserting the data in MySql DB
    single_playlist_detail = []
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    for ch_data in collection1.find({"channel_information.Channel_Name" : channels},{"_id":0}):
        single_playlist_detail.append(ch_data["playlist_information"])

    single_playlist_df = pd.DataFrame(single_playlist_detail[0])

    for index,row in  single_playlist_df.iterrows():
            published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            published_at_formatted = published_at.strftime('%Y-%m-%d %H:%M:%S')
            insert_query='''insert into playlists(
                                            Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count
                                                ) values(%s,%s,%s,%s,%s,%s)'''
            values=(
                    row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    published_at_formatted,
                    row['Video_Count']
                    )
            try:
                mycursor.execute(insert_query , values)
                mydb.commit()
            except:
                print("playlists values have already been inserted")



def Videos_Table(channels):
    # Creating connection to MySQL DB
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Skramar$13071999",
        database = "YouTube_Data"
    )
    # creating a table in MySql DB to store the playlist data
    mycursor = mydb.cursor()

    mycursor.execute(
                '''create table if not exists videos(              
                            Channel_Name varchar(100),
                            Channel_ID varchar(100) ,
                            Video_ID varchar(100) primary key,
                            Video_Title varchar(150),
                            Tags text,
                            Thumbnail varchar(200),
                            Description text,
                            Published timestamp,
                            Duration time,
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Favorite_count int,
                            Definition varchar(50),
                            Caption_Status varchar(50)
                    )'''
                    )
    mydb.commit()

    # Video Data extraction from MongoDb and converting it into data frame and inserting the data in MySql DB
    single_video_detail = []
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    for ch_data in collection1.find({"channel_information.Channel_Name" : channels},{"_id":0}):
        single_video_detail.append(ch_data["video_information"])

    single_video_df = pd.DataFrame(single_video_detail[0])

    single_video_df['Tags'] = single_video_df['Tags'].apply(lambda x: ', '.join(x) 
    if isinstance(x, list) else x)
    for index,row in single_video_df.iterrows():
                duration_match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', row['Duration'])
                if duration_match:
                    hrs = int(duration_match.group(1) or 0)
                    min = int(duration_match.group(2) or 0)
                    sec = int(duration_match.group(3) or 0)

                    # Formatting duration into HH:MM:SS format without spaces
                    duration_formatted = f"{hrs:02d}:{min:02d}:{sec:02d}"
                else:
                    duration_formatted = '00:00:00'  # Default duration if match is not found
                Published = datetime.strptime(row['Published'], '%Y-%m-%dT%H:%M:%SZ')
                Published_at_formatted = Published.strftime('%Y-%m-%d %H:%M:%S')
                insert_query='''insert into videos(
                            Channel_Name ,
                            Channel_ID  ,
                            Video_ID ,
                            Video_Title ,
                            Tags ,
                            Thumbnail ,
                            Description ,
                            Published ,
                            Duration,
                            Views ,
                            Likes ,
                            Comments ,
                            Favorite_count ,
                            Definition ,
                            Caption_Status 
                            ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(
                        row['Channel_Name'],
                        row['Channel_ID'],
                        row['Video_ID'],
                        row['Video_Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['Description'],
                    Published_at_formatted,
                        duration_formatted,
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_count'],
                        row['Definition'],
                        row['Caption_Status']
                        )
                try:
                    mycursor.execute(insert_query,values)
                    mydb.commit()
                except:
                    print("video values already inserted")


def Comments_Table(channels):
    # Creating connection to MySQL DB
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "Skramar$13071999",
        database = "YouTube_Data"
    )
    # creating a table in MySql DB to store the playlist data
    mycursor = mydb.cursor()

    mycursor.execute(
                '''create table if not exists comments(              
                        comment_Id varchar(100) primary key,
                        Video_id varchar(100),
                        Comment_text text,
                        Comment_Author varchar(50),
                        Comment_Published timestamp
                    )'''
                    )
    mydb.commit()

    # Comment Data extraction from MongoDb and converting it into data frame and inserting the data in MySql DB
    single_comments_details = []
    db.client["YouTube_Data"]
    collection1 = db["Channel_Details"]

    for com_data in collection1.find({"channel_information.Channel_Name":channels},{"_id":0}):
        single_comments_details.append(com_data["comment_information"])
    single_comments_df = pd.DataFrame(single_comments_details[0])

    for index , row in single_comments_df.iterrows():
        Published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ')
        Comment_Published_at_formatted = Published.strftime('%Y-%m-%d %H:%M:%S')
        insert_query = '''insert into comments( 
                                                comment_Id ,
                                                Video_id ,
                                                Comment_text,
                                                Comment_Author,
                                                Comment_Published
                                            ) 
                                            values(%s , %s , %s , %s , %s)'''

        values = (row['comment_Id'],
                row['Video_id'],
                row['Comment_text'],
                row['Comment_Author'],
                Comment_Published_at_formatted
                )
        
        try:
            mycursor.execute(insert_query , values)
            mydb.commit()
        except:
            print("comment values have already been inserted")


 
def Create_Tables(channels):
    news = Channels_Table(channels)
    if news:
        return news
    else:
        Playlists_Table(channels)
        Videos_Table(channels)
        Comments_Table(channels)

        return "Tables created Successfully"

def Show_Channel_Tabel():
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    channel_list = []

    for ch_data in collection1.find({},{"_id":0,"channel_information" : 1}):
        channel_list.append(ch_data["channel_information"])
    df_st = st.dataframe(channel_list)
    return df_st


def Show_Playlist_Table():
     db = client["YouTube_Data"]
     collection1 = db["Channel_Details"]
     playlist_list = []

     for pl_data in collection1.find({},{"_id":0,"playlist_information" : 1 , }):
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])
     df1_st = st.dataframe(playlist_list)
     return df1_st


def Show_Videos_Table():
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    video_list = []

    for vi_data in collection1.find({},{"_id":0,"video_information" : 1 , }):
        for i in range(len(vi_data["video_information"])):
            video_list.append(vi_data["video_information"][i])
    df2_st = st.dataframe(video_list)
    return df2_st


def Show_Comments_Table():
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    comment_list = []

    for com_data in collection1.find({},{"_id":0,"comment_information" : 1 , }):
        for i in range(len(com_data["comment_information"])):
            comment_list.append(com_data["comment_information"][i])
    df3_st = st.dataframe(comment_list)
    return df3_st


# Streamlit part
with st.expander(":Red[Skills Used]"):
    st.title(":violet[YouTube Data Harvesting]")
    st.header(":violet[Skill Take Away]")
    st.caption(":red[Python Scripting]")
    st.caption(":red[Data Integration]")
    st.caption(":red[Data Collection]")
    st.caption(":red[NoSQL DB]")
    st.caption(":red[API Integration]")
    st.caption(":red[Data Management]")

channel_id =st.text_input(":violet[Enter the Channel ID]")

if st.button("Get Data") :
    ch_ids = []
    db = client["YouTube_Data"]
    collection1 = db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details already exist")
    else:
        insert = Channel_Details(channel_id)
        st.success(insert)

db = client["YouTube_Data"]
collection1 = db["Channel_Details"]
channel_name_list = []

for ch_data in collection1.find({},{"_id":0,"channel_information" : 1}):
    channel_name_list.append(ch_data["channel_information"]["Channel_Name"])

unique_channel = st.selectbox(":violet[select the channel]" ,  channel_name_list)

if st.button("Migrate"):
    Tables = Create_Tables(unique_channel)
    if Tables == "Tables created Successfully":
        st.success("Table created Successfully")
    else:
        st.success("Table already exist")

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "Skramar$13071999",
    database = "YouTube_Data"
)
mycursor = mydb.cursor()

#show_tables = st.radio(":violet[Select the table]" , ("CHANNELS" , "PLAYLISTS" , "VIDEOS" , "COMMENTS"))

tabs = ["Channel Details", "Playlist Details", "Video Details", "Comment Details"]
selected_tab = st.sidebar.radio(":violet[Select Tab]", tabs)

if selected_tab == "Channel Details":
    Show_Channel_Tabel()
elif selected_tab == "Playlist Details":
    Show_Playlist_Table()
elif selected_tab == "Video Details":
    Show_Videos_Table()
elif selected_tab == "Comment Details":
    Show_Comments_Table()


with st.expander("Queries"):
    question = st.selectbox(
        ":red[Select your question]",
        (
            "Select the question this drop down",
            "1.What are the names of all the videos and their corresponding channels?",
            "2.Which channels have the most number of videos, and how many videos do they have?",
            "3.What are the top 10 most viewed videos and their respective channels?",
            "4.How many comments were made on each video, and what are their corresponding video names?",
            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
            "8.What are the names of all the channels that have published videos in the year 2022?",
            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10.Which videos have the highest number of comments, and what are their corresponding channel names?",
        ),
    )
    if question == "1.What are the names of all the videos and their corresponding channels?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Video_Title as videos  , Channel_Name as ChannelName from videos''')
            t1 = mycursor.fetchall()
            DataFrame1 = pd.DataFrame(t1 , columns = ["video title" , "channel name"])
            mydb.commit()
        st.write(DataFrame1)

    elif question == "2.Which channels have the most number of videos, and how many videos do they have?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Channel_Name as channelname , 
            Total_Videos as no_of_videos from channels
                                order by Total_Videos desc''')
            t2 = mycursor.fetchall()
            DataFrame2 = pd.DataFrame(t2 , columns = ["channel name" , "No Of videos"])
            mydb.commit()
        st.write(DataFrame2)


    elif question == "3.What are the top 10 most viewed videos and their respective channels?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select 
            Views as views , Channel_Name as channelname , 
            Video_Title from videos where Views is not null order by views desc limit 10''')
            t3 = mycursor.fetchall()
            DataFrame3 = pd.DataFrame(t3 , columns = ["Views","channel name" , "videoTitle"])
            mydb.commit()
        st.write(DataFrame3)

    elif question == "4.How many comments were made on each video, and what are their corresponding video names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Comments as no_of_comments , Video_Title as VideosTitle from videos where Comments is not null''')
            t4 = mycursor.fetchall()
            DataFrame4 = pd.DataFrame(t4 , columns = ["Number_of Comments","videoTitle"])
            mydb.commit()
        st.write(DataFrame4)

    elif question ==  "5.Which videos have the highest number of likes, and what are their corresponding channel names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Video_Title as VideosTitle , Channel_Name as channelname , Likes as Like_count from videos where Likes is not null order by likes desc''')
            t5 = mycursor.fetchall()
            DataFrame5 = pd.DataFrame(t5 , columns = ["videoTitle" , "channel name" , "likecount"])
            mydb.commit()
        st.write(DataFrame5)

    elif question == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Likes as Like_count  , Video_Title as VideoTitle from videos''')
            t6 = mycursor.fetchall()
            DataFrame6 = pd.DataFrame(t6 , columns = ["Like_Count" , "VideoTitle"])
            mydb.commit()
        st.write(DataFrame6)

    elif question =="7.What is the total number of views for each channel, and what are their corresponding channel names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Channel_Name as channelname, Views as totalViews from channels''')
            t7 = mycursor.fetchall()
            DataFrame7 = pd.DataFrame(t7 , columns = ["channel name" , "totalViews"])
            mydb.commit()
        st.write(DataFrame7)

    elif question == "8.What are the names of all the channels that have published videos in the year 2022?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Video_Title as VideoTitle ,  Published as Video_release , Channel_Name as channelname from videos where extract(year from Published) = 2022''')
            t8 = mycursor.fetchall()
            DataFrame8 = pd.DataFrame(t8 , columns = ["VideoTitle" , "Video_release"  , "channelname"])
            mydb.commit()
        st.write(DataFrame8)

    elif question =="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Channel_Name as channelname , AVG(Duration) as Average_Duration from videos group by Channel_Name''')
            t9 = mycursor.fetchall()
            DataFrame9 = pd.DataFrame(t9 , columns = ["channelname" , "Average_Duration"])
            mydb.commit()
        
            T9 = []
            for index , row in DataFrame9.iterrows():
                Channel_Titles = row["channelname"]
                average_duration = row["Average_Duration"]
                average_duration_str = str(average_duration)
                T9.append(dict(channel_title = Channel_Titles , avg_duration = average_duration_str))
            Duration_Df = pd.DataFrame(T9)
        st.write(DataFrame9)

    elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?" :
        with st.spinner("Fetching data..."):
            mycursor.execute( '''select Video_Title as VideoTitle , Channel_Name as channelname , Comments as comments  from videos where Comments is not null order by Comments desc''')
            t10 = mycursor.fetchall()
            DataFrame10 = pd.DataFrame(t10 , columns = ["VideoTitles" , "channelname" , "comments"])
            mydb.commit()
        st.write(DataFrame10)

