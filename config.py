import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("ANON_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# API Keys
YOUTUBE_API_KEY = "AIzaSyApdOlgsOeLDVUTQ-X0umh4FJSzKnpzUZE"
GEMINI_API_KEY = "AIzaSyCAUo9x38yHZdb3trhpZX-Oo0cj8IwKkLg"
CUSTOM_SEARCH_API_KEY = "AIzaSyApdOlgsOeLDVUTQ-X0umh4FJSzKnpzUZE"
CUSTOM_SEARCH_ENGINE_ID = "63b236594fab24697"

# YouTube channel configuration
CHANNEL_HANDLE = "@Trustified-Certification"
CHANNEL_URL = "https://www.youtube.com/@Trustified-Certification"

