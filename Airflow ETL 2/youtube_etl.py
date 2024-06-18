import io
import time
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# S3 bucket configuration
S3_BUCKET_NAME = 'krunal-laheri'
S3_FOLDER_PATH = 'Youtube Video Analysis Row Data/'

# AWS credentials
ACCESS_KEY = 'ACCESS_KEY_ID'
SECRET_KEY = 'SECRET_KEY'
REGION_NAME = 'REGION_NAME'

# YouTube API configuration
API_KEY = 'API_KEY'
LIVE_VIDEO_ID = 'LIVE_VIDEO_ID'


def upload_to_s3(dataframe, bucket, folder_path, access_key, secret_key, region_name, object_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )

    try:
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)
        s3_object_name = folder_path + object_name
        s3_client.put_object(Bucket=bucket, Key=s3_object_name, Body=csv_buffer.getvalue())
        print(f"Successfully uploaded {object_name} to s3://{bucket}/{s3_object_name}")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False
    return True

def get_live_chat_id(youtube, video_id):
    response = youtube.videos().list(part='liveStreamingDetails', id=video_id).execute()
    return response['items'][0]['liveStreamingDetails']['activeLiveChatId']

def get_channel_details(youtube, channel_id):
    response = youtube.channels().list(part='snippet,statistics,contentDetails', id=channel_id).execute()
    channel_info = {
        'channel_id': channel_id,
        'channel_name': response['items'][0]['snippet']['title'],
        'channel_url': 'https://www.youtube.com/channel/' + channel_id,
        'profile_picture_url': response['items'][0]['snippet']['thumbnails']['default']['url'],
        'subscriber_count': response['items'][0]['statistics'].get('subscriberCount', 0),
        'view_count': response['items'][0]['statistics'].get('viewCount', 0),
        'video_count': response['items'][0]['statistics'].get('videoCount', 0),
        'custom_url': response['items'][0]['snippet'].get('customUrl', None),
        'channel_description': response['items'][0]['snippet']['description']
    }
    return channel_info

def get_live_video_details(youtube, video_id):
    response = youtube.videos().list(part='snippet,liveStreamingDetails,statistics', id=video_id).execute()
    video_info = {
        'video_id': video_id,
        'title': response['items'][0]['snippet']['title'],
        'description': response['items'][0]['snippet']['description'],
        'channel_id': response['items'][0]['snippet']['channelId'],
        'channel_title': response['items'][0]['snippet']['channelTitle'],
        'published_at': response['items'][0]['snippet']['publishedAt'],
        'view_count': response['items'][0]['statistics'].get('viewCount', 0),
        'like_count': response['items'][0]['statistics'].get('likeCount', 0),
        'dislike_count': response['items'][0]['statistics'].get('dislikeCount', 0),
        'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
        'live_status': response['items'][0]['snippet']['liveBroadcastContent'],
        'actual_start_time': response['items'][0]['liveStreamingDetails'].get('actualStartTime', None),
        'scheduled_start_time': response['items'][0]['liveStreamingDetails'].get('scheduledStartTime', None),
        'concurrent_viewers': response['items'][0]['liveStreamingDetails'].get('concurrentViewers', 0)
    }
    return video_info

def run_youtube_etl():
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    live_chat_id = get_live_chat_id(youtube, LIVE_VIDEO_ID)

    next_page_token = None
    comments_list = []
    channel_details_list = []
    video_info_list = []
    channels_seen = set()

    while True:
        response = youtube.liveChatMessages().list(
            liveChatId=live_chat_id,
            part='snippet,authorDetails',
            pageToken=next_page_token
        ).execute()
        
        for item in response['items']:
            author_name = item['authorDetails']['displayName']
            author_channel_id = item['authorDetails']['channelId']
            message_text = item['snippet']['displayMessage']
            published_at = item['snippet']['publishedAt']
            comments_list.append({
                "author": author_name,
                "author_channel_id": author_channel_id,
                "message": message_text,
                "timestamp": published_at
            })
            if author_channel_id not in channels_seen:
                channel_info = get_channel_details(youtube, author_channel_id)
                channel_details_list.append(channel_info)
                channels_seen.add(author_channel_id)
        
        next_page_token = response.get('nextPageToken')

        # Upload comments to S3
        if len(comments_list) >= 20:
            comments_df = pd.DataFrame(comments_list)
            file_name = 'youtube_comments_' + datetime.now().strftime("%d-%m-%Y-%H-%M-%S") + '.csv'
            upload_to_s3(comments_df, S3_BUCKET_NAME, S3_FOLDER_PATH, ACCESS_KEY, SECRET_KEY, REGION_NAME, file_name)
            comments_list = []
        
        # Upload channel details to S3
        if len(channel_details_list) >= 20:
            channel_details_df = pd.DataFrame(channel_details_list)
            file_name = 'youtube_channel_details_' + datetime.now().strftime("%d-%m-%Y-%H-%M-%S") + '.csv'
            upload_to_s3(channel_details_df, S3_BUCKET_NAME, S3_FOLDER_PATH, ACCESS_KEY, SECRET_KEY, REGION_NAME, file_name)
            channel_details_list = []

        # Upload video details to S3 at regular intervals
        video_info = get_live_video_details(youtube, LIVE_VIDEO_ID)
        video_info_list.append(video_info)
        if len(video_info_list) >= 10:
            video_details_df = pd.DataFrame(video_info_list)
            file_name = 'youtube_video_details_' + datetime.now().strftime("%d-%m-%Y-%H-%M-%S") + '.csv'
            upload_to_s3(video_details_df, S3_BUCKET_NAME, S3_FOLDER_PATH, ACCESS_KEY, SECRET_KEY, REGION_NAME, file_name)
            video_info_list = []

        time.sleep(5)

# run_youtube_etl()
