# YouTube Video Processor

This project fetches YouTube videos from the Trustified-Certification channel, processes them with Gemini AI to generate reports, fetches product images via Google Custom Search, and stores everything in Supabase.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your Supabase credentials:
```
SUPABASE_URL=your_supabase_url
ANON_KEY=your_anon_key
```

## Usage

### 1. Fetch All Videos (One-time setup)

Run this script once to fetch all existing videos from the YouTube channel and store them in Supabase:

```bash
python fetch_videos.py
```

This will:
- Connect to YouTube API
- Fetch all videos from @Trustified-Certification channel
- Store video URLs, release dates, and set status to 'pending' in Supabase

### 2. Run Cron Job (Every 20 minutes)

This script should be scheduled to run every 20 minutes. It performs two tasks:

```bash
python cron_job.py
```

**Task 1: Process Pending Videos**
- Selects the oldest pending video
- Updates status to 'updating'
- Sends video URL to Gemini AI for analysis
- Stores the report in the 'reports' table
- Fetches product image using Google Custom Search
- Updates image_url and sets status to 'completed'

**Task 2: Check for New Videos**
- Fetches latest 3 videos from YouTube
- Compares with latest 3 videos in database
- Adds any new videos with status 'pending'

### 3. Configure Gemini Prompt

Open `cron_job.py` and find the section marked with:

```python
# ============================================================
# TODO: REPLACE THIS PROMPT WITH YOUR ACTUAL PROMPT
# ============================================================
```

Replace the placeholder prompt with your specific instructions for Gemini AI.

## Scheduling the Cron Job (Windows)

1. Open Task Scheduler
2. Create a new task
3. Set trigger to run every 20 minutes
4. Set action to run: `python C:\path\to\cron_job.py`

## Database Tables

### videos
- id (BIGSERIAL PRIMARY KEY)
- video_id (TEXT, UNIQUE)
- video_url (TEXT)
- channel_id (TEXT)
- published_at (TIMESTAMPTZ)
- status (TEXT: 'pending', 'updating', 'completed', 'failed')
- retry_count (INT)
- last_attempt_at (TIMESTAMPTZ)
- created_at (TIMESTAMPTZ)
- updated_at (TIMESTAMPTZ)

### reports
- id (BIGINT, references videos.id)
- video_url (TEXT, UNIQUE)
- results (JSONB)
- image_url (TEXT)
- image_status (TEXT: 'pending', 'completed', 'failed')
- created_at (TIMESTAMPTZ)
- updated_at (TIMESTAMPTZ)

## API Keys

All API keys are configured in `config.py`:
- YouTube Data API v3
- Google Gemini AI
- Google Custom Search API


