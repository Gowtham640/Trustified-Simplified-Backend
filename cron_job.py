from googleapiclient.discovery import build
import google.genai as genai
from google.genai import types
from datetime import datetime, timezone
import config
import time
import requests
import os
import time

def retry_supabase_operation(operation_func, max_retries=3, delay=2):
    """
    Retry Supabase operations with exponential backoff for network issues
    """
    for attempt in range(max_retries):
        try:
            return operation_func()
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                print(f"Supabase operation failed (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Supabase operation failed after {max_retries} attempts: {e}")
                raise e

def get_gemini_usage_count():
    """Get current Gemini API usage count for today"""
    try:
        # Create usage file path
        usage_file = 'gemini_usage.txt'

        # Check if file exists and is from today
        if os.path.exists(usage_file):
            with open(usage_file, 'r') as f:
                lines = f.readlines()
                if len(lines) >= 2:
                    date_str = lines[0].strip()
                    count_str = lines[1].strip()

                    # Check if it's today's date
                    today = datetime.now().strftime('%Y-%m-%d')
                    if date_str == today:
                        return int(count_str)

        # If file doesn't exist or is not from today, reset to 0
        return 0
    except Exception as e:
        print(f"Error reading usage count: {e}")
        return 0

def increment_gemini_usage_count():
    """Increment and save Gemini API usage count"""
    try:
        usage_file = 'gemini_usage.txt'
        today = datetime.now().strftime('%Y-%m-%d')
        current_count = get_gemini_usage_count()
        new_count = current_count + 1

        # Write date and count
        with open(usage_file, 'w') as f:
            f.write(f"{today}\n{new_count}\n")

        return new_count
    except Exception as e:
        print(f"Error updating usage count: {e}")
        return 0

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
        def update_video_status():
            return config.supabase.table('videos').update({
                'status': 'updating',
                'last_attempt_at': datetime.now(timezone.utc).isoformat(),
                'retry_count': video.get('retry_count', 0) + 1
            }).eq('id', video_id).execute()

        retry_supabase_operation(update_video_status)
        
        # Generate reports with Gemini (returns array of reports)
        print("Generating reports with Gemini...")
        reports_array = generate_report_with_gemini(video_url)

        if not reports_array or len(reports_array) == 0:
            raise Exception("Failed to generate reports")

        print(f"Generated {len(reports_array)} product report(s)")

        # Insert each report into reports table as separate rows
        for i, report_json in enumerate(reports_array):
                # Extract data from Gemini results for database fields
                product_id_value = report_json.get('product_id', '')
                product_info = report_json.get('product_info', {})
                basic_tests = report_json.get('basic_tests', {})

                # Safely extract fields with proper defaults
                company_name = product_info.get('company_name', '').strip()
                if not company_name:
                    company_name = 'Unknown Company'

                product_name = product_info.get('product_name', '').strip()
                product_category = product_info.get('product_category', '').strip()

                # Determine overall verdict from individual test results
                verdict_value = 'fail'  # Default to fail
                if basic_tests:
                    # Check if any test has a result field
                    test_results = []
                    for test_key, test_data in basic_tests.items():
                        if isinstance(test_data, dict) and 'result' in test_data:
                            result = test_data['result'].strip().lower()
                            test_results.append(result)

                    # If we have test results, determine overall verdict
                    if test_results:
                        # If all results are "passed", then overall verdict is "pass"
                        if all(result == 'passed' for result in test_results):
                            verdict_value = 'pass'
                        else:
                            verdict_value = 'fail'
                    else:
                        # Fallback: check for top-level verdict field
                        verdict_value = basic_tests.get('verdict', 'fail').strip().lower()

                # Final validation against allowed values
                allowed_verdicts = ['pass', 'fail', 'not assigned', 'pending']
                if verdict_value not in allowed_verdicts:
                    verdict_value = 'fail'  # Default fallback

                # Create unique ID for each product report
                # Generate unique report ID (video_id + product_index to ensure uniqueness)
                report_id = f"{video_id}_{i}"

                print(f"Storing report {i+1}/{len(reports_array)}: {product_id_value or report_id}")

                try:
                    # Insert report with all required fields per updated schema
                    def insert_report():
                        return config.supabase.table('reports').insert({
                            'id': report_id,  # Unique report ID (video_id + index)
                            'results': report_json,
                            'image_status': 'pending',
                            'video_url': video_url,
                            'product_id': product_id_value,  # Text field (was bigint)
                            'product_name': product_name,
                            'product_category': product_category,
                            'verdict': verdict_value,  # Now allows: pass, fail, not assigned, pending
                            'company': company_name,
                            'video_id': video_id  # Foreign key to videos table
                        }).execute()

                    result = retry_supabase_operation(insert_report)
                    inserted_report_id = report_id

                except Exception as insert_error:
                    print(f"Error inserting report {i+1}: {insert_error}")
                    continue

                # Fetch product image for this specific report
                print(f"Fetching product image for {product_id_value or product_name}...")
                image_url = fetch_product_image(report_json)

                # Update report with image
                try:
                    if image_url:
                        def update_image_success():
                            return config.supabase.table('reports').update({
                                'image_url': image_url,
                                'image_status': 'completed'
                            }).eq('id', inserted_report_id).execute()
                        retry_supabase_operation(update_image_success)
                        print(f"Image stored for {product_id_value or product_name}: {image_url}")
                    else:
                        def update_image_failed():
                            return config.supabase.table('reports').update({
                                'image_status': 'failed'
                            }).eq('id', inserted_report_id).execute()
                        retry_supabase_operation(update_image_failed)
                        print(f"Failed to fetch product image for {product_id_value or product_name}")
                except Exception as update_error:
                    print(f"Error updating image status for {product_id_value or product_name}: {update_error}")

        # Update video status to 'completed'
        def update_video_completed():
            return config.supabase.table('videos').update({
                'status': 'completed'
            }).eq('id', video_id).execute()

        retry_supabase_operation(update_video_completed)
        print(f"Successfully processed video: {video_url}")

    except Exception as e:
        print(f"Error processing video: {e}")
        try:
            # Update video status to 'failed'
            if 'video_id' in locals():
                def update_video_failed():
                    return config.supabase.table('videos').update({
                        'status': 'failed'
                    }).eq('id', video_id).execute()
                retry_supabase_operation(update_video_failed)
        except Exception as failure_error:
            print(f"Failed to update video status to failed: {failure_error}")

def generate_report_with_gemini(video_url):
    """
    Send video URL to Gemini and get report in JSON format
    Uses native YouTube URL support with FileData to make Gemini actually "see" the video
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

        # Create the analysis prompt that forces video content analysis
        prompt = """
        ### SYSTEM ROLE ###
You are a specialized JSON generator for laboratory analysis. You must perform a visual analysis of the provided video content. If you cannot access the video directly to see specific lab equipment, technician gear, and product texture, you must set "can_access_url": false.

### INSTRUCTIONS ###
1. Count how many different products are tested in the video
2. For each product, analyze: lab results, company info, and quality tests
3. Number each product starting from "1", "2", etc.

### JSON STRUCTURE AND STRICT ORDERING ###
You MUST output valid JSON. The keys MUST appear in this exact order:
1. "debug_info"
2. "product_id"
3. "product_info"
4. "basic_tests"
5. "contaminant_tests"
6. "review"

### DATA RULES ###
- Product ID: [COMPANY][NAME][FLAVOR] (ALL CAPS, NO SPACES).
- Categories: Whey Concentrate, Whey Isolate, Whey Blend, Plant protein, Creatine, Food, Omega 3, Others.
- No ranges: Provide single average values.
- Sub-tests: Must include "verdict", "claimed", and "tested".
- CRUCIAL FIELDS: Always include "company_name", "product_name", "product_category", "verdict" , "price" , "price_per_serving", "serving_size" in product_info.
- PROTEIN/CREATINE: If product is whey/creatine, include "protein_per_serving"/"creatine_per_serving" in basic_tests.
- Note: Only tests must include a note explaining the result of the test and not sub tests.
- Verdict: Every test and subtest must have a "verdict" field(pass/fail/NULL).
- NULL: If any field doesnt have the information then mark it "NULL".
- Claimed: Only basic_tests subtests must have claimed field. Contaminant and review tests must not have claimed field.
- Review: Must include "taste","mixability","packaging","serving_size_accuracy". 

### INPUT VIDEO ###
""" + video_url + """

### OUTPUT TEMPLATE (STRICT ADHERENCE REQUIRED) ###
{"1": {
  "debug_info": { ... },
  "product_id": "...",
  "product_info": { ... },
  "basic_tests": { ... },
  "contaminant_tests": { ... },
  "review": { ... }
},
"2": {...}
}

        """

        from google.genai import types

        # 1. Create Part object for native YouTube video processing
        print(f"Creating Part object for YouTube URL: {video_url}")
        video_part = types.Part.from_uri(
            file_uri=video_url,
            mime_type='video/mp4'
        )
        print(f"Part created successfully: {video_part}")

        # 2. Define the config using the proper class
        gemini_config = types.GenerateContentConfig(
            temperature=0.1,
            tools=[
                types.Tool(
                    google_search=types.GoogleSearch()
                )
            ]
        )

        # 3. Call the model with Part object and prompt
        print("Sending request to Gemini API with video Part...")

        # Track Gemini usage
        usage_count = increment_gemini_usage_count()
        emoji = "🤖"
        if usage_count >= 18:
            emoji = "⚠️"
        elif usage_count >= 15:
            emoji = "🟡"
        print(f"{emoji} GEMINI API CALL #{usage_count}/20 {emoji}")

        if usage_count >= 20:
            print("⚠️ WARNING: You have reached your Gemini API quota limit! ⚠️")

        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=[video_part, prompt],  # Pass video Part AND prompt
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

        # Parse JSON response - handle both single and multiple products
        try:
            print("Attempting to parse JSON response...")

            # First try to parse the entire response as one JSON object/array
            try:
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

                # If it's a single object, check if it contains numbered products
                if isinstance(reports_array, dict):
                    # Check if it has numbered product keys like "1", "2", "3"
                    product_keys = [k for k in reports_array.keys() if k.isdigit()]
                    if product_keys:
                        # New structure: numbered product keys at top level
                        products_array = []
                        for key in sorted(product_keys, key=int):  # Sort numerically
                            products_array.append(reports_array[key])
                        print(f"Extracted {len(products_array)} products from numbered keys: {product_keys}")
                        return products_array
                    elif 'products' in reports_array and isinstance(reports_array['products'], dict):
                        # Alternative structure: products object with numbered keys
                        products_obj = reports_array['products']
                        products_array = []
                        for key in sorted(products_obj.keys()):  # Sort by key to maintain order
                            products_array.append(products_obj[key])
                        print(f"Extracted {len(products_array)} products from products object")
                        return products_array
                    else:
                        # Old single product structure
                        print("Returning single report as array")
                        return [reports_array]
                # If it's already an array, return as is
                elif isinstance(reports_array, list):
                    print(f"Returning {len(reports_array)} reports")
                    return reports_array

            except json.JSONDecodeError as single_parse_error:
                # If single parse fails, try to split and parse multiple JSON objects
                print(f"Single JSON parse failed: {single_parse_error}")
                print("Attempting to parse as multiple JSON objects...")

                # Split response into individual JSON objects
                # Look for pattern: } followed by { (end of one object, start of next)
                json_objects = []
                current_pos = 0

                while current_pos < len(response_text):
                    # Find the start of a JSON object
                    start_pos = response_text.find('{', current_pos)
                    if start_pos == -1:
                        break

                    # Find the matching closing brace
                    brace_count = 0
                    end_pos = start_pos
                    for i in range(start_pos, len(response_text)):
                        if response_text[i] == '{':
                            brace_count += 1
                        elif response_text[i] == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break

                    if brace_count != 0:
                        break  # Malformed JSON

                    # Extract and parse this JSON object
                    json_str = response_text[start_pos:end_pos].strip()
                    if json_str:
                        try:
                            parsed_obj = json.loads(json_str)
                            json_objects.append(parsed_obj)
                            print(f"Successfully parsed JSON object {len(json_objects)}")
                        except json.JSONDecodeError as obj_error:
                            print(f"Failed to parse JSON object at position {start_pos}: {obj_error}")
                            break

                    current_pos = end_pos

                if json_objects:
                    print(f"Successfully parsed {len(json_objects)} JSON objects")
                    return json_objects
                else:
                    print("Failed to parse any valid JSON objects")
                    return None

        except Exception as e:
            print(f"Unexpected error during JSON parsing: {e}")
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
        print(f"Custom Search API response status: {response.status_code}")

        if response.status_code == 400:
            print(f"Bad Request details: {response.text}")
            print("This usually means invalid API key or search engine ID")
            print("SOLUTION: Go to Google Cloud Console and check/renew your API keys")
            return None
        elif response.status_code == 403:
            print(f"Forbidden - likely API key expired or quota exceeded: {response.text}")
            print("SOLUTION: Check API key validity and billing status in Google Cloud Console")
            return None

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

            # Filter out shorts (videos <= 160 seconds)
            for search_result in search_results:
                video_id = search_result['video_id']
                duration = video_durations.get(video_id, 0)

                if duration > 160:
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
                    def insert_video():
                        return config.supabase.table('videos').insert({
                            'video_id': video['video_id'],
                            'video_url': video['video_url'],
                            'channel_id': video['channel_id'],
                            'published_at': video['published_at'],
                            'status': 'pending'
                        }).execute()
                    retry_supabase_operation(insert_video)
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

    # Display current Gemini usage
    current_usage = get_gemini_usage_count()
    emoji = "🤖"
    if current_usage >= 18:
        emoji = "⚠️"
    elif current_usage >= 15:
        emoji = "🟡"
    print(f"{emoji} CURRENT GEMINI USAGE: {current_usage}/20 {emoji}")

    # Part 1: Process pending videos
    process_pending_video()
    
    # Part 2: Check for new videos
    check_new_videos()
    
    print(f"\n{'='*50}")
    print(f"CRON JOB COMPLETED - {datetime.now()}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()


