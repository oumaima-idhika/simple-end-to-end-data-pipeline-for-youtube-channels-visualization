#import libraries
import requests
import pandas as pd
import s3fs
import time

API_KEY = "AI*******************************"

def get_video_details(video_id):
    url = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+API_KEY
    response = requests.get(url).json()
 
    return response['items'][0]['statistics']['viewCount'],response['items'][0]['statistics']['likeCount'],response['items'][0]['statistics']['favoriteCount'],response['items'][0]['statistics']['commentCount']

def get_videos(channel_name , channel_id ):
    pageToken = ""
    df = pd.DataFrame(columns=["channel_name" , "Video_id","Video_title","Upload_date","View_count","Like_count","Favorite_count","Comment_count"])
    while 1:
        url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+channel_id+"&part=snippet,id&order=date&maxResults=10000&"+pageToken

        response = requests.get(url).json()
        time.sleep(1) #give it a second before starting the for loop
        for video in response['items']:
            if video['id']['kind'] == "youtube#video":
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                video_title = str(video_title).replace("&amp;","")
                upload_date = video['snippet']['publishedAt']
                upload_date = str(upload_date).split("T")[0]
                # print(upload_date)
                view_count, like_count, favorite_count, comment_count = get_video_details(video_id)

                df = df.append({'channel_name':channel_name , 'Video_id':video_id,'Video_title':video_title,
                               "Upload_date":upload_date,"View_count":view_count,
                               "Like_count":like_count,"Favorite_count":favorite_count,
                               "Comment_count":comment_count}, ignore_index=True)
        try:
            if response['nextPageToken'] != None: 
                pageToken = "pageToken=" + response['nextPageToken']

        except:
            break

    df.to_csv(f"s3://youtube-fetched-data/input_data/{channel_name}.csv" , index=False)
    
