from googleapiclient.discovery import build
from datetime import datetime
import config
import re

def parse_duration_to_seconds(duration):
    """
    Parse ISO 8601 duration format (PT#H#M#S) to seconds
    Example: PT4M13S -> 253 seconds
    """
    # Pattern matches: PT (hours)H (minutes)M (seconds)S
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration)

    if not match:
        return 0

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def get_channel_id(channel_handle):
    """
    Convert channel handle (e.g., @Trustified-Certification) to channel ID
    """
    youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
    
    # Remove @ if present
    handle = channel_handle.lstrip('@')
    
    try:
        request = youtube.search().list(
            part='snippet',
            q=handle,
            type='channel',
            maxResults=1
        )
        response = request.execute()
        
        if response['items']:
            channel_id = response['items'][0]['snippet']['channelId']
            print(f"Found channel ID: {channel_id}")
            return channel_id
        else:
            print(f"No channel found for handle: {channel_handle}")
            return None
    except Exception as e:
        print(f"Error getting channel ID: {e}")
        return None

def fetch_all_videos(channel_id):
    """
    Fetch all videos from the YouTube channel, filtering out shorts (<= 60 seconds)
    Returns list of video dictionaries with video_id, video_url, channel_id, and published_at
    """
    youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
    videos = []
    next_page_token = None

    try:
        while True:
            request = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                order='date',
                type='video',
                pageToken=next_page_token
            )
            response = request.execute()

            # Collect video IDs from search results
            video_ids = []
            search_results = []

            for item in response['items']:
                video_id = item['id']['videoId']
                published_at = item['snippet']['publishedAt']
                video_ids.append(video_id)
                search_results.append({
                    'video_id': video_id,
                    'video_url': f"https://www.youtube.com/watch?v={video_id}",
                    'channel_id': channel_id,
                    'published_at': published_at
                })

            # Get duration details for all videos in this batch
            if video_ids:
                # YouTube API limits to 50 video IDs per request
                video_details_request = youtube.videos().list(
                    part='contentDetails',
                    id=','.join(video_ids[:50])  # Limit to 50 as per API limits
                )
                video_details_response = video_details_request.execute()

                # Create mapping of video_id to duration
                video_durations = {}
                for video_detail in video_details_response['items']:
                    duration_iso = video_detail['contentDetails']['duration']
                    duration_seconds = parse_duration_to_seconds(duration_iso)
                    video_durations[video_detail['id']] = duration_seconds

                # Filter out shorts (videos <= 60 seconds) and add to results
                for search_result in search_results:
                    video_id = search_result['video_id']
                    duration = video_durations.get(video_id, 0)

                    if duration > 60:
                        videos.append(search_result)
                        print(f"Included video: {search_result['video_url']} (duration: {duration}s)")
                    else:
                        print(f"Skipped short: {search_result['video_url']} (duration: {duration}s)")

            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

            print(f"Fetched {len(videos)} long-form videos so far...")

        print(f"Total long-form videos fetched: {len(videos)}")
        return videos

    except Exception as e:
        print(f"Error fetching videos: {e}")
        return videos

def store_videos(videos):
    """
    Insert videos into Supabase with status='pending'
    """
    if not videos:
        print("No videos to store")
        return
    
    try:
        # Insert videos, ignoring duplicates
        for video in videos:
            try:
                result = config.supabase.table('videos').insert({
                    'video_id': video['video_id'],
                    'video_url': video['video_url'],
                    'channel_id': video['channel_id'],
                    'published_at': video['published_at'],
                    'status': 'pending'
                }).execute()
                print(f"Stored video: {video['video_url']}")
            except Exception as e:
                # Skip if video already exists (unique constraint violation)
                if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
                    print(f"Video already exists: {video['video_url']}")
                else:
                    print(f"Error storing video {video['video_url']}: {e}")
        
        print(f"Finished storing videos")
        
    except Exception as e:
        print(f"Error storing videos: {e}")

def main():
    """
    Main execution: Fetch and store all videos from the channel
    """
    print(f"Starting video fetch for channel: {config.CHANNEL_HANDLE}")
    
    # Get channel ID
    channel_id = get_channel_id(config.CHANNEL_HANDLE)
    if not channel_id:
        print("Failed to get channel ID. Exiting.")
        return
    
    # Fetch all videos
    videos = fetch_all_videos(channel_id)
    
    # Store videos in Supabase
    store_videos(videos)
    
    print("Video fetch completed!")

if __name__ == "__main__":
    main()


