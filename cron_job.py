from googleapiclient.discovery import build
import google.genai as genai
from google.genai import types
from datetime import datetime, timezone
import config
import time
import requests

def process_pending_video():
    """
    Part 1: Process the newest pending video
    - Get newest pending video
    - Generate report with Gemini
    - Fetch product image
    - Update database
    """
    print("\n=== PROCESSING PENDING VIDEOS ===")

    try:
        # Get newest pending video (sorted by published_at descending)
        result = config.supabase.table('videos').select('*').eq('status', 'pending').order('published_at', desc=True).limit(1).execute()
        
        if not result.data or len(result.data) == 0:
            print("No pending videos to process")
            return
        
        video = result.data[0]
        video_id = video['id']
        video_url = video['video_url']
        
        print(f"Processing video: {video_url}")
        
        # Update status to 'updating'
        config.supabase.table('videos').update({
            'status': 'updating',
            'last_attempt_at': datetime.now(timezone.utc).isoformat(),
            'retry_count': video.get('retry_count', 0) + 1
        }).eq('id', video_id).execute()
        
        # Generate reports with Gemini (returns array of reports)
        print("Generating reports with Gemini...")
        reports_array = generate_report_with_gemini(video_url)

        if not reports_array or len(reports_array) == 0:
            raise Exception("Failed to generate reports")

        print(f"Generated {len(reports_array)} product report(s)")

        # Insert each report into reports table as separate rows
        for i, report_json in enumerate(reports_array):
            # Create unique ID for each product report
            product_id = report_json.get('product_id', f"{video_id}_{i}")

            print(f"Storing report {i+1}/{len(reports_array)}: {product_id}")

            try:
                # Use video's database ID as foreign key (since reports.id references videos.id)
                # For multiple products per video, we'll need to modify schema later
                # For now, use video_id and overwrite with latest product
                result = config.supabase.table('reports').insert({
                    'id': video_id,  # Foreign key to videos table
                    'video_url': video_url,
                    'results': report_json,
                    'image_status': 'pending'
                }).execute()

                inserted_report_id = video_id

            except Exception as insert_error:
                print(f"Error inserting report {i+1}: {insert_error}")
                continue

            # Fetch product image for this specific report
            print(f"Fetching product image for {product_id}...")
            image_url = fetch_product_image(report_json)

            # Update report with image
            try:
                if image_url:
                    config.supabase.table('reports').update({
                        'image_url': image_url,
                        'image_status': 'completed'
                    }).eq('id', inserted_report_id).execute()
                    print(f"Image stored for {product_id}: {image_url}")
                else:
                    config.supabase.table('reports').update({
                        'image_status': 'failed'
                    }).eq('id', inserted_report_id).execute()
                    print(f"Failed to fetch product image for {product_id}")
            except Exception as update_error:
                print(f"Error updating image status for {product_id}: {update_error}")
        
        # Update video status to 'completed'
        config.supabase.table('videos').update({
            'status': 'completed'
        }).eq('id', video_id).execute()
        
        print(f"Successfully processed video: {video_url}")
        
    except Exception as e:
        print(f"Error processing video: {e}")
        try:
            # Update video status to 'failed'
            if 'video_id' in locals():
                config.supabase.table('videos').update({
                    'status': 'failed'
                }).eq('id', video_id).execute()
        except:
            pass

def generate_report_with_gemini(video_url):
    """
    Send video URL to Gemini and get report in JSON format
    """
    try:
        print(f"Initializing Gemini client for video: {video_url}")
        client = genai.Client(api_key=config.GEMINI_API_KEY)

        # Debug: List available models
        print("Listing available models...")
        try:
            models = client.models.list()
            print("Available models:")
            for model in models:
                print(f"  - {model.name}")
        except Exception as list_error:
            print(f"Failed to list models: {list_error}")
        
        # ============================================================
        # TODO: REPLACE THIS PROMPT WITH YOUR ACTUAL PROMPT
        # ============================================================
        prompt = """
        Task: Analyze the YouTube video at: [[INSERT_URL_HERE]] and generate a laboratory analysis report.

Output Format: You MUST return a valid JSON (or a list of JSON objects if multiple products are found). The schema MUST follow this exact key order and structure:

product_id: (COMPANY + NAME + FLAVOR) in ALL CAPS, no spaces.

product_info: {product_name, product_category, serving_size, verdict}

basic_tests: {result, ...sub-tests}

contaminant_tests: {result, ...sub-tests}

review: {result, ...details}

Strict Logic Rules:

Categories: Only use: Whey Concentrate, Whey Isolate, Whey Blend, Plant protein, Creatine, Food, Omega 3, Others.

Calculation: If the video only provides percentages for Protein/Creatine, you MUST calculate the per_serving value based on the serving_size.

No Ranges: Use single values only. If a range is given, provide the average.

Sub-test Structure: Every nutrient/test (protein, carbs, etc.) MUST contain three keys: "result" (Pass/Fail), "claimed", and "tested".

Ordering: The JSON keys MUST appear in the exact order: product_id -> product_info -> basic_tests -> contaminant_tests -> review.
{
  "product_id": "BRANDNAMEPRODUCTNAMEFLAVOR",
  "product_info": {
    "product_name": "Example Protein",
    "product_category": "Whey Concentrate",
    "serving_size": "1 Scoop (30g)",
    "verdict": "Pass"
  },
  "basic_tests": {
    "result": "Pass",
    "protein": { "result": "Pass", "claimed": "24g", "tested": "24.5g" }
  },
  "contaminant_tests": {
    "result": "Pass",
    "heavy_metals": { "result": "Pass", "status": "Below LOQ" }
  },
  "review": {
    "result": "Pass",
    "taste": { "result": "Pass", "description": "Good" }
  }
}

        """
        # ============================================================
        
        # Create the complete prompt with URL
        full_prompt = f"""
        You are a JSON generator for laboratory analysis reports. Always output valid JSON only, no conversational text or markdown.

        CRITICAL ORDERING REQUIREMENT: You MUST output the JSON keys in this EXACT order:
        1. debug_info (first)
        2. product_id (second)
        3. product_info (third)
        4. basic_tests (fourth)
        5. contaminant_tests (fifth)
        6. review (last)

        FAILURE TO FOLLOW THIS ORDER WILL RESULT IN INVALID OUTPUT.

        JSON STRUCTURE RULES:
        - debug_info: contains debugging information about URL access and analysis
        - product_id: (COMPANY + NAME + FLAVOR) in ALL CAPS, no spaces
        - product_info: contains product_name, product_category, serving_size, verdict
        - basic_tests: contains result, and sub-tests with claimed/tested values
        - contaminant_tests: contains result, and contamination test results
        - review: contains result, and subjective analysis (taste, texture, etc.)
        - All nested objects must have "result": "Pass"/"Fail" fields where applicable
        - Use single values only, no ranges
        - If percentages given for protein/creatine, calculate per_serving values

        DEBUGGING REQUIREMENTS:
        Include a "debug_info" object as the FIRST field that contains:
        - can_access_url: true/false - whether you can actually access the video content
        - url_access_method: "web_search"/"direct_video_access"/"metadata_only"/"none"
        - video_info_found: object with title, description, and any other metadata you can access
        - product_identification_method: "video_content_analysis"/"metadata_inference"/"web_search_results"/"assumption"
        - confidence_level: "high"/"medium"/"low"/"none" - how confident you are in the product identification
        - reasoning: detailed explanation of how you determined the product

        Task: Analyze the YouTube video at: {video_url} and generate a laboratory analysis report.

        Output Format: You MUST return a valid JSON with the schema following this exact key order and structure:

        product_id: (COMPANY + NAME + FLAVOR) in ALL CAPS, no spaces.

        product_info: {{product_name, product_category, serving_size, verdict}}

        basic_tests: {{result, ...sub-tests}}

        contaminant_tests: {{result, ...sub-tests}}

        review: {{result, ...details}}

        Strict Logic Rules:

        Categories: Only use: Whey Concentrate, Whey Isolate, Whey Blend, Plant protein, Creatine, Food, Omega 3, Others.

        Calculation: If the video only provides percentages for Protein/Creatine, you MUST calculate the per_serving value based on the serving_size.

        No Ranges: Use single values only. If a range is given, provide the average.

        Sub-test Structure: Every nutrient/test (protein, carbs, etc.) MUST contain three keys: "result" (Pass/Fail), "claimed", and "tested".

        Ordering: The JSON keys MUST appear in the exact order: debug_info -> product_id -> product_info -> basic_tests -> contaminant_tests -> review.

        {{
          "debug_info": {{
            "can_access_url": false,
            "url_access_method": "web_search",
            "video_info_found": {{
              "title": "Video title from search",
              "description": "Video description from search"
            }},
            "product_identification_method": "metadata_inference",
            "confidence_level": "low",
            "reasoning": "Cannot access actual video content, inferred from title/description only"
          }},
          "product_id": "BRANDNAMEPRODUCTNAMEFLAVOR",
          "product_info": {{
            "product_name": "Example Protein",
            "product_category": "Whey Concentrate",
            "serving_size": "1 Scoop (30g)",
            "verdict": "Pass"
          }},
          "basic_tests": {{
            "result": "Pass",
            "protein": {{ "result": "Pass", "claimed": "24g", "tested": "24.5g" }}
          }},
          "contaminant_tests": {{
            "result": "Pass",
            "heavy_metals": {{ "result": "Pass", "status": "Below LOQ" }}
          }},
          "review": {{
            "result": "Pass",
            "taste": {{ "result": "Pass", "description": "Good" }}
          }}
        }}
        """

        from google.genai import types

        # 1. Define the config using the proper class
        gemini_config = types.GenerateContentConfig(
            temperature=0.1,
            tools=[
                types.Tool(
                    google_search=types.GoogleSearch()
                )
            ]
        )

        # 2. Call the model
        print("Sending request to Gemini API...")
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=full_prompt,
                config=gemini_config
            )
            print("Gemini API call successful")
        except Exception as api_error:
            print(f"Gemini API call failed: {api_error}")
            print(f"API Error type: {type(api_error).__name__}")
            return None

        print("Received response from Gemini, parsing...")

        # Parse JSON from response
        import json
        try:
            print(f"Response object type: {type(response)}")
            print(f"Response object attributes: {dir(response) if response else 'None'}")

            # Try different response structures
            if response and hasattr(response, 'text') and response.text:
                response_text = response.text.strip()
                print(f"Response text length: {len(response_text)}")
                print(f"Response text preview: {response_text[:200]}...")
            elif response and hasattr(response, 'candidates') and response.candidates and len(response.candidates) > 0:
                candidate = response.candidates[0]
                print(f"First candidate: {candidate}")
                if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts') and len(candidate.content.parts) > 0:
                    response_text = candidate.content.parts[0].text.strip()
                    print(f"Response from candidates length: {len(response_text)}")
                    print(f"Response text preview: {response_text[:200]}...")
                else:
                    print("No valid parts found in candidate")
                    return None
            else:
                print(f"Response object: {response}")
                response_text = str(response).strip() if response else ""
                print(f"Response as string length: {len(response_text)}")
                if not response_text:
                    print("Empty response received")
                    return None
        except Exception as e:
            print(f"Error parsing response: {e}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None

        # Remove markdown code blocks if present
        if response_text.startswith('```json'):
            response_text = response_text[7:]
        if response_text.startswith('```'):
            response_text = response_text[3:]
        if response_text.endswith('```'):
            response_text = response_text[:-3]

        response_text = response_text.strip()

        # Try to parse as JSON array first (multiple products)
        try:
            print("Attempting to parse JSON response...")
            reports_array = json.loads(response_text)
            print(f"Successfully parsed JSON. Type: {type(reports_array)}")

            # Debug: Print the actual response structure
            if isinstance(reports_array, dict):
                print("DEBUG: Single report keys:", list(reports_array.keys()))
                if 'debug_info' in reports_array:
                    print("DEBUG: debug_info found:", reports_array['debug_info'])
                else:
                    print("DEBUG: No debug_info field found in response")
                print("DEBUG: product_id:", reports_array.get('product_id', 'NOT FOUND'))
            elif isinstance(reports_array, list) and len(reports_array) > 0:
                print("DEBUG: First report keys:", list(reports_array[0].keys()))
                if 'debug_info' in reports_array[0]:
                    print("DEBUG: debug_info found:", reports_array[0]['debug_info'])
                else:
                    print("DEBUG: No debug_info field found in response")

            # If it's a single object, convert to array for consistency
            if isinstance(reports_array, dict):
                print("Returning single report as array")
                return [reports_array]
            # If it's already an array, return as is
            elif isinstance(reports_array, list):
                print(f"Returning {len(reports_array)} reports")
                return reports_array
        except json.JSONDecodeError as json_error:
            print(f"Failed to parse JSON response: {json_error}")
            print(f"Response text (first 500 chars): {response_text[:500]}...")
            return None

    except Exception as e:
        print(f"Error generating report with Gemini: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def fetch_product_image(report_json):
    """
    Use Google Custom Search API to find product image
    Returns the URL of the full-sized image
    """
    try:
        # Extract product name for search query (from product_info section)
        product_name = report_json.get('product_info', {}).get('product_name', '')

        # Construct search query (no brand field in current structure)
        search_query = product_name.strip()
        
        if not search_query:
            print("No product name found in report")
            return None
        
        # Make Custom Search API request
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'key': config.CUSTOM_SEARCH_API_KEY,
            'cx': config.CUSTOM_SEARCH_ENGINE_ID,
            'q': search_query,
            'searchType': 'image',
            'num': 1
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract image URL from 'link' field in 'items'
        if 'items' in data and len(data['items']) > 0:
            image_url = data['items'][0]['link']
            return image_url
        else:
            print("No image results found")
            return None
            
    except Exception as e:
        print(f"Error fetching product image: {e}")
        return None

def check_new_videos():
    """
    Part 2: Check for new videos on YouTube and add them to database
    - Fetch latest videos from YouTube (filtering out shorts <= 60s)
    - Fetch latest videos from database
    - Compare and add new long-form videos
    """
    print("\n=== CHECKING FOR NEW VIDEOS ===")

    try:
        # Get channel ID (assuming we know it or fetch it)
        from fetch_videos import get_channel_id, parse_duration_to_seconds
        channel_id = get_channel_id(config.CHANNEL_HANDLE)

        if not channel_id:
            print("Failed to get channel ID")
            return

        # Fetch latest videos from YouTube API (get more than 3 to account for shorts filtering)
        youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=10,  # Get more to account for shorts filtering
            order='date',
            type='video'
        )
        response = request.execute()

        # Collect video IDs and search results
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

        # Get duration details for videos
        youtube_videos = []
        if video_ids:
            video_details_request = youtube.videos().list(
                part='contentDetails',
                id=','.join(video_ids[:50])
            )
            video_details_response = video_details_request.execute()

            # Create mapping of video_id to duration
            video_durations = {}
            for video_detail in video_details_response['items']:
                duration_iso = video_detail['contentDetails']['duration']
                duration_seconds = parse_duration_to_seconds(duration_iso)
                video_durations[video_detail['id']] = duration_seconds

            # Filter out shorts (videos <= 60 seconds)
            for search_result in search_results:
                video_id = search_result['video_id']
                duration = video_durations.get(video_id, 0)

                if duration > 60:
                    youtube_videos.append(search_result)
                else:
                    print(f"Skipped short video: {search_result['video_url']} (duration: {duration}s)")

        # Fetch latest videos from database
        db_result = config.supabase.table('videos').select('*').order('published_at', desc=True).limit(3).execute()
        db_videos = db_result.data

        # Compare videos
        db_video_ids = {v['video_id'] for v in db_videos}

        # Find new videos (limit to top 3 long-form videos)
        new_videos = []
        for yt_video in youtube_videos[:3]:  # Only consider the latest 3 long-form videos
            if yt_video['video_id'] not in db_video_ids:
                new_videos.append(yt_video)
        
        # Add new videos to database
        if new_videos:
            print(f"Found {len(new_videos)} new video(s)")
            for video in new_videos:
                try:
                    config.supabase.table('videos').insert({
                        'video_id': video['video_id'],
                        'video_url': video['video_url'],
                        'channel_id': video['channel_id'],
                        'published_at': video['published_at'],
                        'status': 'pending'
                    }).execute()
                    print(f"Added new video: {video['video_url']}")
                except Exception as e:
                    print(f"Error adding video {video['video_url']}: {e}")
        else:
            print("No new videos found")
            
    except Exception as e:
        print(f"Error checking for new videos: {e}")

def main():
    """
    Main cron job execution
    Runs every 20 minutes to:
    1. Process one pending video
    2. Check for new videos
    """
    print(f"\n{'='*50}")
    print(f"CRON JOB STARTED - {datetime.now()}")
    print(f"{'='*50}")
    
    # Part 1: Process pending videos
    process_pending_video()
    
    # Part 2: Check for new videos
    check_new_videos()
    
    print(f"\n{'='*50}")
    print(f"CRON JOB COMPLETED - {datetime.now()}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()


