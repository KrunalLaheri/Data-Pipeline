import time
import json
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# AWS Kinesis configuration
KINESIS_STREAM_NAME = 'YoutubeCommentStream'

# AWS credentials
ACCESS_KEY = 'ACCESS_KEY'
SECRET_KEY = 'SECRET_KEY'
REGION_NAME = 'REGION_NAME'


# YouTube API configuration
API_KEY = 'API_KEY'
LIVE_VIDEO_ID = 'LIVE_VIDEO_ID'

def put_records_to_kinesis(data, stream_name, partition_key, access_key, secret_key, region_name):
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )

    try:
        response = kinesis_client.put_records(
            Records=[
                {
                    'Data': json.dumps(record),
                    'PartitionKey': partition_key
                } for record in data
            ],
            StreamName=stream_name
        )
        # response = kinesis_client.put_records(
        #     Records=[
        #         {
        #             'Data': "krunal",
        #             'PartitionKey': partition_key
        #         } for record in data
        #     ],
        #     StreamName=stream_name
        # )
        print(f"Successfully put {len(data)} records to Kinesis stream {stream_name}")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"Error putting records to Kinesis: {e}")
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

        # Stream comments to Kinesis
        if len(comments_list) >= 20:
            put_records_to_kinesis(comments_list, KINESIS_STREAM_NAME, 'comment', ACCESS_KEY, SECRET_KEY, REGION_NAME)
            comments_list = []
        
        # Stream channel details to Kinesis
        if len(channel_details_list) >= 20:
            put_records_to_kinesis(channel_details_list, KINESIS_STREAM_NAME, 'channel', ACCESS_KEY, SECRET_KEY, REGION_NAME)
            channel_details_list = []

        # Stream video details to Kinesis at regular intervals
        video_info = get_live_video_details(youtube, LIVE_VIDEO_ID)
        video_info_list.append(video_info)
        if len(video_info_list) >= 10:
            put_records_to_kinesis(video_info_list, KINESIS_STREAM_NAME, 'video', ACCESS_KEY, SECRET_KEY, REGION_NAME)
            video_info_list = []

        time.sleep(5)

# Uncomment to run the ETL process
# run_youtube_etl()
