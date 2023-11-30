import pandas as pd
from pymongo import MongoClient 
import sqlite3 
from datetime import datetime
import streamlit as st
from googleapiclient.discovery import build 
import pymysql.connections


#API connection
youtube_api_key = "AIzaSyB0xhzZzDadoB6T0SnF_rKgtZCU6kTVQ9o"
api_service_name = "youtube"
api_version = "v3"
youtube=build(api_service_name,api_version,developerKey=youtube_api_key)

channel_id="UCJcCB-QYPIBcbKcBQOTwhiA"

#getting channel details
def youtubedata(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
    )    
    response = request.execute()
    
    for i in response["items"]:
        channel_detail=dict(Channel_name=i["snippet"]["title"],
                            Channel_id=i['id'],
                            Subscribers=i['statistics']['subscriberCount'],
                            Views=i['statistics']['viewCount'],
                            Total_videos=i['statistics']['videoCount'],
                            Channel_Description=i['snippet']['description'],
                            Playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                   )
        
    
    return channel_detail

#getting video ids
def get_videoid(channel_id):
    video_ids=[]
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id)
    response=request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        request=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=playlist_id,
                                        maxResults=50,
                                        pageToken=next_page_token)
        response=request.execute()
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#getting video details
def get_video_info(Video_ID):
    video_data=[]
    for video_id in Video_ID:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id)
        response=request.execute()

        for item in response['items']:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                     Channel_id=item['snippet']['channelId'],
                     Video_id=item['id'],
                     Title=item['snippet']['title'],
                     Thumbnail=item['snippet']['thumbnails']["default"]['url'],
                     Description=item['snippet'].get('description'),
                     Published_Date=item['snippet']['publishedAt'],
                     Duration=item['contentDetails']['duration'],
                     Views=item['statistics'].get('viewCount'),
                     Likes=item['statistics'].get('likeCount'), 
                     Comments=item['statistics'].get('commentCount'),
                     Favorite_Count=item['statistics']['favoriteCount'],
                     Caption_Status=item['contentDetails']['caption']
                     )
            video_data.append(data)
    return video_data


#getting comment details
def get_comment_info(Video_ID):
    comment_list=[]
    try:
        for videoid in Video_ID:
            request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=videoid,
                    maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                     Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                     Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                     Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                     Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                comment_list.append(data)
    except:
        pass
    return comment_list


#getting playlist details
def get_playlist_info(channel_id):
    playlist_list=[]
    next_page_token=None
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            data=dict(Playlist_id=item['id'],
                     Playlist_title=item['snippet']['title'],
                     Channel_id=item['snippet']['channelId'],
                     Channel_name=item['snippet']['channelTitle'],
                     PublishedAt=item['snippet']['publishedAt'],
                     Video_count=item['contentDetails']['itemCount'])
            playlist_list.append(data)

        next_page_token=response.get('nextPageToken')    
        if next_page_token is None:
            break
    return playlist_list


#MongoDB connection
loc_client = MongoClient("mongodb://localhost:27017/")
db = loc_client['youtube_data']
coll = db['youtube_collection']

def main(channelid):
    c=youtubedata(channel_id)
    p=get_playlist_info(channel_id)
    video_IDs=get_videoid(channel_id)
    v=get_video_info(video_IDs)
    cm=get_comment_info(video_IDs)
    data={"Channel details":c,
         "Playlist details":p,
          "Video details":v,
          "Comment details":cm
         }
    coll=db['youtube_collection']
    coll.insert_one(data)
    return "uploded successfully"



#sql connections
def channel_table():
    con=pymysql.connect(
            host='localhost',
            user='root',
            password='12345',
            database='youtube',
            port=3306,
            autocommit=True
                )
    cursor=con.cursor()

    #multiple channel details 
    drop_query="""drop table if exists channels"""
    cursor.execute(drop_query)

    #SQL table creation(channel details)
    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_id varchar(80) primary key,
                                                            Subscribers int,
                                                            Views int,
                                                            Total_videos int,
                                                            description text,
                                                            playlist_id varchar(80))'''
        cursor.execute(create_query)
    except:
        print("channel table already created")

    #extract data from mongoDB
    ch_list=[]
    for ch_data in coll.find({},{'_id':0,'Channel details':1}):
        ch_list.append(ch_data['Channel details'])
    df=pd.DataFrame(ch_list) 

    #insert query(channel details)
    for index,row in df.iterrows():
        insert_query="""insert into channels(Channel_Name,
                                            Channel_id,
                                            Subscribers,
                                            Views,
                                            Total_videos,
                                            description,
                                            playlist_id)

                                            values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['Channel_name'],
               row['Channel_id'],
               row['Subscribers'],
               row['Views'],
               row['Total_videos'],
               row['Channel_Description'],
               row['Playlist_id'])
        try:
            cursor.execute(insert_query,values)
        except:
            print("channel already inserted")

#playlist table
def playlist_table():
    con=pymysql.connect(
            host='localhost',
            user='root',
            password='12345',
            database='youtube',
            port=3306,
            autocommit=True
                )
    cursor=con.cursor()
    #multiple playlist details 
    drop_query="""drop table if exists playlists"""
    cursor.execute(drop_query)
    
    #SQL table creation(playlist details)

    create_query='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                        Playlist_title text,
                                                        Channel_id varchar(100),
                                                        Channel_name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int)'''
    cursor.execute(create_query)
    
    #extract data from mongoDB
    pl_list=[]
    for pl_data in coll.find({},{'_id':0,'Playlist details':1}):
        for i in range(len(pl_data['Playlist details'])):
            pl_list.append(pl_data['Playlist details'][i])
    df1=pd.DataFrame(pl_list)
    df1['PublishedAt'] = pd.to_datetime(df1['PublishedAt'], format='%Y-%m-%dT%H:%M:%SZ')
    
    #insert query(playlist details)
    for index,row in df1.iterrows():
        insert_query="""insert into playlists(Playlist_id,
                                        Playlist_title,
                                        Channel_id,
                                        Channel_name,
                                        PublishedAt,
                                        Video_count)
                                        
                                        values(%s,%s,%s,%s,%s,%s)"""
        values=(row['Playlist_id'],
           row['Playlist_title'],
           row['Channel_id'],
           row['Channel_name'],
           row['PublishedAt'],
           row['Video_count'])

        cursor.execute(insert_query,values)
    

#video table
def video_table():
    con=pymysql.connect(
            host='localhost',
            user='root',
            password='12345',
            database='youtube',
            port=3306,
            autocommit=True
                )
    cursor=con.cursor()

    #multiple video details 
    drop_query="""drop table if exists videos"""
    cursor.execute(drop_query)

    #SQL table creation(video details)

    create_query='''create table if not exists videos(Channel_name varchar(100),
                                                     Channel_id varchar(100),
                                                     Video_id varchar(80) primary key,
                                                     Title text,
                                                     Thumbnail text,
                                                     Description text,
                                                     Published_Date timestamp,
                                                     Duration int,
                                                     Views int,
                                                     Likes int, 
                                                     Comments int,
                                                     Favorite_Count int,
                                                     Caption_Status varchar(80)
                                                     )'''
    cursor.execute(create_query)

    #extract data from mongoDB
    vd_list=[]
    for vd_data in coll.find({},{'_id':0,'Video details':1}):
        for i in range(len(vd_data['Video details'])):
            vd_list.append(vd_data['Video details'][i])
    df2=pd.DataFrame(vd_list)
    df2['Published_Date'] = pd.to_datetime(df2['Published_Date'], format='%Y-%m-%dT%H:%M:%SZ')
    df2['Duration'] = pd.to_timedelta(df2['Duration']).dt.total_seconds().astype(int)

    #insert query(video details)
    for index,row in df2.iterrows():
        insert_query="""insert into videos(Channel_name,
                                         Channel_id,
                                         Video_id,
                                         Title,
                                         Thumbnail,
                                         Description,
                                         Published_Date,
                                         Duration,
                                         Views,
                                         Likes, 
                                         Comments,
                                         Favorite_Count,
                                         Caption_Status) 

                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['Channel_name'],
            row['Channel_id'],
            row['Video_id'],
            row['Title'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Duration'],
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Favorite_Count'],
            row['Caption_Status'])
        cursor.execute(insert_query,values)

#comment table
def comment_table():
    con=pymysql.connect(
            host='localhost',
            user='root',
            password='12345',
            database='youtube',
            port=3306,
            autocommit=True
                )
    cursor=con.cursor()

    #multiple comment details 
    drop_query="""drop table if exists comments"""
    cursor.execute(drop_query)

    #SQL table creation(comment details)

    create_query='''create table if not exists comments(Comment_id varchar(100) primary key,
                                                        Video_id varchar(80),
                                                        Comment_text text,
                                                        Comment_Author text,
                                                        Comment_published timestamp
                                                        )'''
    cursor.execute(create_query)

    #extract data from mongoDB
    co_list=[]
    for co_data in coll.find({},{'_id':0,'Comment details':1}):
        for i in range(len(co_data['Comment details'])):
            co_list.append(co_data['Comment details'][i])
    df3=pd.DataFrame(co_list)
    df3['Comment_published'] = pd.to_datetime(df3['Comment_published'], format='%Y-%m-%dT%H:%M:%SZ')

     #insert query(video details)
    for index,row in df3.iterrows():
        insert_query="""insert into comments(Comment_id,
                                            Video_id,
                                            Comment_text,
                                            Comment_Author,
                                            Comment_published)

                                        values(%s,%s,%s,%s,%s)"""
        values=(row['Comment_id'],
            row['Video_id'],
            row['Comment_text'],
            row['Comment_Author'],
            row['Comment_published'])
        cursor.execute(insert_query,values)

def alltables():
    channel_table()
    playlist_table()
    video_table()
    comment_table()

    return "Table Created Successfully"

def show_channel_table():
    ch_list=[]
    for ch_data in coll.find({},{'_id':0,'Channel details':1}):
        ch_list.append(ch_data['Channel details'])
    st_df=st.dataframe(ch_list)

    return st_df

def show_playlist_table():
    pl_list=[]
    for pl_data in coll.find({},{'_id':0,'Playlist details':1}):
        for i in range(len(pl_data['Playlist details'])):
            pl_list.append(pl_data['Playlist details'][i])
    st_df1=st.dataframe(pl_list)

    return st_df1

def show_video_table():
    vd_list=[]
    for vd_data in coll.find({},{'_id':0,'Video details':1}):
        for i in range(len(vd_data['Video details'])):
            vd_list.append(vd_data['Video details'][i])
    st_df2=st.dataframe(vd_list)

    return st_df2

def show_comment_table():
    co_list=[]
    for co_data in coll.find({},{'_id':0,'Comment details':1}):
        for i in range(len(co_data['Comment details'])):
            co_list.append(co_data['Comment details'][i])
    st_df3=st.dataframe(co_list)

    return st_df3

#streamlit

with st.sidebar:
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.header('Skills Take Away')
    st.caption("Python Scripting")
    st.caption("Data Collectionn")
    st.caption("MongoDB")
    st.caption("API Intergration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the Channel ID")
if st.button("Collect and store Data"):
    ch_ids=[]
    db = loc_client['youtube_data']
    for ch_data in coll.find({},{'_id':0,'Channel details':1}):
        ch_ids.append(ch_data["Channel details"]["Channel_id"])

    if channel_id in ch_ids:
        st.success('Channel Id Already Exist')

    else:
        insert=main(channel_id)
        st.success(insert)
    
if st.button("Migrate to Sql"):
    Tabel=alltables()
    st.success(Tabel)

show_table=st.radio("Select the table",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()
if show_table=="PLAYLISTS":
    show_playlist_table()
if show_table=="VIDEOS":
    show_video_table()
if show_table=="COMMENTS": 
    show_comment_table() 

#sql connection
con=pymysql.connect(
        host='localhost',
        user='root',
        password='12345',
        database='youtube',
        port=3306,
        autocommit=True
            )
cursor=con.cursor()

Questions=st.selectbox("Select the question",("1. All Videos and their Channel name",
                                              "2. Channel with most number of videos",
                                              "3. Top 10 most viewed Videos",
                                              "4. Comments in each Videos",
                                              "5. Most Liked Videos",
                                              "6. Likes of all Videos",
                                              "7. Views of each Channel",
                                              "8. Videos published in the year of 2023",
                                              "9. Average Duration of all videos in each channels",
                                              "10. Videos with highest number of comments"))

if Questions=="1. All Videos and their Channel name":
    query1="""select title as videos,Channel_name as channelname from videos"""
    cursor.execute(query1)
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)

elif Questions=="2. Channel with most number of videos":
    query2="""select Channel_Name as channelname,total_videos as no_videos from channels
                    order by total_videos desc"""
    cursor.execute(query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel name","No of videos"])
    st.write(df2)

elif Questions=="3. Top 10 most viewed Videos":
    query3="""select views as views,channel_name as channelname,title as videotitle from videos
                    where views is not null order by views desc limit 10"""
    cursor.execute(query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name",'videotitle'])
    st.write(df3)

elif Questions=="4. Comments in each Videos":
    query4="""select comments as no_comments,title as videotitle from videos where comments is not null """
    cursor.execute(query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of Comments","Video title"])
    st.write(df4)

elif Questions=="5. Most Liked Videos":
    query5="""select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc"""
    cursor.execute(query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Video title","Channel name","likecount"])
    st.write(df5)

elif Questions=="6. Likes of all Videos":
    query6="""select likes as likecount,title as videotitle from videos"""
    cursor.execute(query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","Video title"])
    st.write(df6)

elif Questions=="7. Views of each Channel":
    query7="""select channel_name as channelname,views as totalviews from channels"""
    cursor.execute(query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel name","Total views"])
    st.write(df7)

elif Questions=="8. Videos published in the year of 2023":
    query8="""select title as Video_title,Published_Date as videoreleased,channel_name as channelname from videos
                where extract(year from Published_Date)=2023"""
    cursor.execute(query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video title","Published Date","Channel name"])
    st.write(df8)

elif Questions=="9. Average Duration of all videos in each channels":
        query9="""select channel_name as channelname,avg(duration) as averageduration from videos
                group by channel_name"""
        cursor.execute(query9)
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["Channel name","Average duration"])
        st.write(df9)

elif Questions=="10. Videos with highest number of comments":
        query10="""select title as videotitle,channel_name as channelname,comments as comments from videos
                where comments is not null order by comments desc"""
        cursor.execute(query10)
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Video title","Channel name","Comments"])
        st.write(df10)