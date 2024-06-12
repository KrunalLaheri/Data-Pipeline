from googleapiclient.discovery import build
import pandas as pd

# Initialize YouTube API
api_key = 'AIzaSyBZP8qZGFFWxFs20ERm-M-VnVxMsF-UcDs'


youtube = build('youtube', 'v3', developerKey=api_key)



# Function to get live chat ID
def get_live_chat_id(video_id):
    """
    Retrieves the live chat ID associated with a live YouTube video.

    Args:
        video_id (str): The ID of the YouTube video.

    Returns:
        str: The live chat ID.
    """
    response = youtube.videos().list(part='liveStreamingDetails', id=video_id).execute()
    live_chat_id = response['items'][0]['liveStreamingDetails']['activeLiveChatId']
    return live_chat_id

# Function to get live chat messages
def get_live_chat_messages(live_chat_id):
    """
    Retrieves live chat messages from a live YouTube video.

    Args:
        live_chat_id (str): The ID of the live chat.

    Returns:
        pandas.DataFrame: DataFrame containing live chat messages.
    """
    messages = []
    response = youtube.liveChatMessages().list(
        liveChatId=live_chat_id,
        part='snippet,authorDetails',
        maxResults=200
    ).execute()
    
    for item in response['items']:
        msg = {
            'author_name': item['authorDetails']['displayName'],
            'author_channel_id': item['authorDetails']['channelId'],
            'message': item['snippet']['displayMessage'],
            'published_at': item['snippet']['publishedAt']
        }
        messages.append(msg)
    
    return pd.DataFrame(messages)

# Function to get channel details
def get_channel_details(channel_id):
    """
    Retrieves details about a YouTube channel.

    Args:
        channel_id (str): The ID of the YouTube channel.

    Returns:
        dict: Dictionary containing channel details.
    """
    response = youtube.channels().list(
        part='snippet,statistics,contentDetails',
        id=channel_id
    ).execute()
    
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
    
    # Fetch public playlists
    playlists_response = youtube.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=50
    ).execute()
    
    playlists = []
    for playlist_item in playlists_response['items']:
        playlist = {
            'playlist_id': playlist_item['id'],
            'playlist_title': playlist_item['snippet']['title'],
            'playlist_url': 'https://www.youtube.com/playlist?list=' + playlist_item['id']
        }
        playlists.append(playlist)
    
    channel_info['playlists'] = playlists
    
    # Add country if available
    if 'country' in response['items'][0]['snippet']:
        channel_info['country'] = response['items'][0]['snippet']['country']
    else:
        channel_info['country'] = None
    
    return channel_info


# Function to get details of a live video
def get_live_video_details(video_id):
    """
    Retrieves comprehensive details about a live YouTube video.

    Args:
        video_id (str): The ID of the YouTube video.

    Returns:
        dict: Dictionary containing video details.
    """
    response = youtube.videos().list(
        part='snippet,liveStreamingDetails,statistics',
        id=video_id
    ).execute()
    
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

# Example usage
video_id = 'pK_tzOO84j0'
live_chat_id = get_live_chat_id(video_id)
messages_df = get_live_chat_messages(live_chat_id)

# Create a DataFrame to store channel details
channel_details = []

# Extract unique channel IDs from the messages DataFrame
unique_channel_ids = messages_df['author_channel_id'].unique()

# Fetch channel details for each unique channel ID
for channel_id in unique_channel_ids:
    channel_info = get_channel_details(channel_id)
    channel_details.append(channel_info)

# Convert the list of dictionaries into a DataFrame
channel_details_df = pd.DataFrame(channel_details)

# Fetch details about the live video
video_details = get_live_video_details(video_id)

# Convert the video details dictionary into a DataFrame
video_details_df = pd.DataFrame([video_details])

# Save the user comments, channel details, and video details to separate CSV files
messages_df.to_csv('Youtube Extracted Data/youtube_user_comments.csv', index=False)
channel_details_df.to_csv('Youtube Extracted Data/youtube_channel_details.csv', index=False)
video_details_df.to_csv('Youtube Extracted Data/youtube_video_details.csv', index=False)