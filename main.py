#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import pickle
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from openai import OpenAI, OpenAIError
from retry import retry
from datetime import datetime

# Load environment variables
load_dotenv(find_dotenv(), override=True)

# Constants
IDENTIFIER = "\n#atualizado"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
CLIENT_SECRETS_FILE = os.getenv("YOUTUBE_CLIENT_SECRETS_FILE")
CREDENTIALS_PICKLE_FILE = "credentials.pkl"

# Authenticate OpenAI API
openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# YouTube Categories
CATEGORY_DICT = {2: "Autos & Vehicles", 1: "Film & Animation", 10: "Music", 15: "Pets & Animals",
                 17: "Sports", 18: "Short Movies", 19: "Travel & Events", 20: "Gaming", 21: "Videoblogging",
                 22: "People & Blogs", 23: "Comedy", 24: "Entertainment", 25: "News & Politics",
                 26: "Howto & Style", 27: "Education", 28: "Science & Technology", 29: "Nonprofits & Activism"}

def get_authenticated_service():
    """Authenticate YouTube API and return the service instance."""
    creds = None
    if os.path.exists(CREDENTIALS_PICKLE_FILE):
        with open(CREDENTIALS_PICKLE_FILE, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(CREDENTIALS_PICKLE_FILE, "wb") as token:
            pickle.dump(creds, token)

    return build("youtube", "v3", credentials=creds)

def get_video_list(youtube, upload_id):
    """Fetches all video IDs from a YouTube channel's upload playlist."""
    video_list = []
    request = youtube.playlistItems().list(part="snippet,contentDetails", playlistId=upload_id, maxResults=50)
    
    while request:
        response = request.execute()
        video_list.extend([item['contentDetails']['videoId'] for item in response['items']])
        request = youtube.playlistItems().list_next(request, response)

    return video_list

def get_video_details(youtube, video_list):
    """Fetches metadata for up to 50 YouTube videos at a time."""
    stats_list = []
    for i in range(0, len(video_list), 50):
        request = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_list[i:i+50])
        response = request.execute()
        
        for video in response.get("items", []):
            snippet, stats = video.get("snippet", {}), video.get("statistics", {})
            stats_list.append({
                "title": snippet.get("title", ""),
                "published": snippet.get("publishedAt", ""),
                "description": snippet.get("description", ""),
                "tags": snippet.get("tags", []),
                "categoryId": int(snippet.get("categoryId", 0)),
                "video_url": f"https://www.youtube.com/watch?v={video['id']}",
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "video_length": video.get("contentDetails", {}).get("duration", "")
            })

    return stats_list

def calculate_minutes(duration):
    """Converts YouTube video duration (ISO 8601) to minutes."""
    try:
        hours = int(duration.split("H")[0].replace("PT", "")) if "H" in duration else 0
        minutes = int(duration.split("H")[-1].split("M")[0]) if "M" in duration else 0
    except:
        hours, minutes = 0, 0
    return hours * 60 + minutes

@retry(tries=3, delay=2)
def extract_data(channels, api_key):
    """Extracts YouTube video metadata for all channels."""
    youtube = build("youtube", "v3", developerKey=api_key)
    df_videos = []

    for channel_name, channel_info in channels.items():
        print(f"Extracting data from {channel_name}")
        video_data = get_video_details(youtube, get_video_list(youtube, channel_info["id"]))
        df = pd.DataFrame(video_data)

        # Data Cleaning & Processing
        df["channel_name"] = channel_name
        df["categoryId"] = df["categoryId"].map(CATEGORY_DICT.get)
        df["video_length"] = df["video_length"].map(calculate_minutes)
        df["published"] = pd.to_datetime(df["published"])
        df["days_from_publish"] = (datetime.now() - df["published"]).dt.days.clip(lower=1)
        df["views_per_day"] = df["view_count"] // df["days_from_publish"]

        df_videos.append(df)

    return pd.concat(df_videos, ignore_index=True)

def update_youtube_videos(df):
    """Updates video descriptions and tags on YouTube."""
    youtube = get_authenticated_service()
    failed_rows = []

    for _, row in df.iterrows():
        video_id = row["video_url"].split("v=")[-1]
        new_description = row["new_description"] if isinstance(row["new_description"], str) else ""
        new_tags = row["new_tags"].split(", ") if isinstance(row["new_tags"], str) else []

        if IDENTIFIER in row["description"] or len(new_description) > 5000 or len(", ".join(new_tags)) > 500:
            failed_rows.append(row)
            continue

        try:
            youtube.videos().update(
                part="snippet",
                body={"id": video_id, "snippet": {"description": new_description, "tags": new_tags, "title": row["title"]}}
            ).execute()
            print(f"Updated: {row['title']}")
        except Exception as e:
            print(f"Failed: {row['title']} - {e}")
            failed_rows.append(row)

        time.sleep(0.5)  # API Rate Limiting

    return pd.DataFrame(failed_rows)

def generate_ai_response(prompt):
    """Generic function to call OpenAI API for descriptions or tags."""
    retries = 3
    for _ in range(retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "system", "content": "SEO YouTube Assistant"}, {"role": "user", "content": prompt}],
                temperature=0.3,
                timeout=30,
            )
            return response.choices[0].message.content.strip('"\'')
        except OpenAIError as e:
            print(f"Error: {e}. Retrying...")
            time.sleep(5)
    return ""

if __name__ == "__main__":
    CHANNELS = {"xadrezBrasil": {"id": "UC5K-TQsItHnNLjqYf8A3CTw"}}
    df = extract_data(CHANNELS, os.getenv("YOUTUBE_API_KEY"))
    df = df[~df["description"].str.contains(IDENTIFIER, na=False)]

    if not df.empty:
        df["new_description"] = df["title"].apply(lambda t: generate_ai_response(f"SEO descrição para: {t}"))
        df["new_tags"] = df["title"].apply(lambda t: generate_ai_response(f"20 tags otimizadas para: {t}"))
        failed_updates = update_youtube_videos(df)
        if not failed_updates.empty:
            failed_updates.to_excel("failed_videos.xlsx", index=False)

    print("YouTube update complete!")
