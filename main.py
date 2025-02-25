#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import random
import pickle
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from openai import OpenAI, OpenAIError
from retry import retry
from datetime import date

# Load environment variables
load_dotenv(find_dotenv(), override=True)

# Constants
IDENTIFIER = "\n#atualizado"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
CLIENT_SECRETS_FILE = os.getenv("YOUTUBE_CLIENT_SECRETS_FILE")
CREDENTIALS_PICKLE_FILE = "credentials.pkl"

# Authenticate OpenAI API
openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

CATEGORY_DICT = {
        2: "Autos & Vehicles",
        1: "Film & Animation",
        10: "Music",
        15: "Pets & Animals",
        17: "Sports",
        18:  "Short Movies",
        19:  "Travel & Events",
        20:  "Gaming",
        21:  "Videoblogging",
        22:  "People & Blogs",
        23:  "Comedy",
        24:  "Entertainment",
        25:  "News & Politics",
        26:  "Howto & Style",
        27:  "Education",
        28:  "Science & Technology",
        29:  "Nonprofits & Activism",
        30:  "Movies",
        31:  "Anime/Animation",
        32:  "Action/Adventure",
        33:  "Classics",
        34:  "Comedy",
        35:  "Documentary",
        36:  "Drama",
        37:  "Family",
        38:  "Foreign",
        39:  "Horror",
        40:  "Sci-Fi/Fantasy",
        41:  "Thriller",
        42:  "Shorts",
        43:  "Shows",
        44:  "Trailers"
        }

def get_video_list(youtube, upload_id):
    video_list = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=upload_id,
        maxResults=50
    )
    next_page = True
    while next_page:
        response = request.execute()
        data = response['items']

        for video in data:
            video_id = video['contentDetails']['videoId']
            if video_id not in video_list:
                video_list.append(video_id)

        # Do we have more pages?
        if 'nextPageToken' in response.keys():
            next_page = True
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=upload_id,
                pageToken=response['nextPageToken'],
                maxResults=50
            )
        else:
            next_page = False

    return video_list


# Once we have our video list we can pass it to this function to get details.
# Again we have a max of 50 at a time so we will use a for loop to break up our list. 

def get_video_details(youtube, video_list):
    stats_list=[]

    # Can only get 50 videos at a time.
    for i in range(0, len(video_list), 50):
        request= youtube.videos().list(
            part="snippet,contentDetails,statistics,status,topicDetails",
            id=video_list[i:i+50]
        )

        data = request.execute()
        for video in data['items']:
            title=video['snippet']['title']
            published=video['snippet']['publishedAt']
            description=video['snippet']['description']
            try: 
                tags = video['snippet']['tags']
                tag_count= len(video['snippet']['tags'])
            except:
                tags = []
                tag_count = 0
            thumb_url=video['snippet']['thumbnails']['high']['url']
            video_url = 'https://www.youtube.com/watch?v={0}'.format(video['id'])
            categoryId=video['snippet']['categoryId']
            contentDetails=video['contentDetails']
            video_length_minutes=video['contentDetails']['duration']
            view_count=video['statistics'].get('viewCount',0)
            like_count=video['statistics'].get('likeCount',0)
            dislike_count=video['statistics'].get('dislikeCount',0)
            comment_count=video['statistics'].get('commentCount',0)
            try:
                topicDetails = video['topicDetails']
            except:
                topicDetails = ""
                
            stats_dict=dict(title=title, description=description, published=published, tags = tags, tag_count=tag_count, thumb_url = thumb_url,
                            video_url=video_url, categoryId=categoryId, contentDetails=contentDetails, video_length_minutes=video_length_minutes, 
                            view_count=view_count, like_count=like_count, dislike_count=dislike_count, comment_count=comment_count, 
                            topicDetails=topicDetails)
            stats_list.append(stats_dict)

    return stats_list


def calculate_minutes(x):
    try:
        hours = int((x[2:].split('H')[0]))
    except:
        hours = 0
    try:
        minutes = int((x[2:].split('H')[-1].split('M')[0]))
    except:
        minutes = 0
    return hours*60 + minutes

def return_category_name(CATEGORY_DICT, categoryId):
    categoryId = int(categoryId)
    return (CATEGORY_DICT[categoryId])

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

@retry(tries=3, delay=2)
def extract_data(channels, api_key, category_dict):
    """Extracts YouTube video data."""
    youtube = build("youtube", "v3", developerKey=api_key)
    df_videos_final = []

    for channel_name, channel_info in channels.items():
        print(f"Extracting data from channel {channel_name}")

        # Get Video Details
        video_data = get_video_details(youtube, get_video_list(youtube, channel_info["id"]))
        df_videos = pd.DataFrame(video_data)

        # Process Data
        df_videos["channel_name"] = channel_name
        df_videos["search_date"] = date.today()
        df_videos["categoryId"] = df_videos["categoryId"].map(lambda x: return_category_name(category_dict, x))
        df_videos["video_length_minutes"] = df_videos["video_length_minutes"].map(calculate_minutes)
        df_videos["published"] = pd.to_datetime(df_videos["published"])
        df_videos["days_from_publish"] = (pd.Timestamp.now() - df_videos["published"]).dt.days.clip(lower=1)
        df_videos["views_per_lifetime_days"] = df_videos["view_count"] // df_videos["days_from_publish"]
        df_videos["reactions"] = df_videos["like_count"] + df_videos["dislike_count"] + df_videos["comment_count"]

        df_videos_final.append(df_videos)

    return pd.concat(df_videos_final, ignore_index=True)

def update_youtube_videos(df):
    """Updates YouTube video descriptions and tags."""
    youtube = get_authenticated_service()
    failed_rows = []

    for _, row in df.iterrows():
        video_id = row["video_url"].split("v=")[-1]
        new_description = row["new_description"] if isinstance(row["new_description"], str) else ""
        new_tags = row["new_suggested_tags"].split(", ") if isinstance(row["new_suggested_tags"], str) else []

        # Skip already updated videos
        if IDENTIFIER in str(row["description"]):
            continue

        if len(new_description) > 5000 or len(", ".join(new_tags)) > 500:
            failed_rows.append(row)
            continue

        # Update video metadata
        request = youtube.videos().update(
            part="snippet",
            body={"id": video_id, "snippet": {"categoryId": "27", "description": new_description, "tags": new_tags, "title": row["title"]}},
        )

        try:
            request.execute()
            print(f"Updated video: {row['title']}")
        except Exception as e:
            print(f"Failed to update {row['title']}: {e}")
            failed_rows.append(row)

        time.sleep(0.5)  # API rate limiting

    return pd.DataFrame(failed_rows)

def generate_ai_response(prompt):
    """Generic function to get a response from OpenAI API."""
    retries = 5
    for _ in range(retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "system", "content": "Você é especialista em SEO para vídeos do YouTube."}, {"role": "user", "content": prompt}],
                temperature=0.3,
                timeout=30,
            )
            return response.choices[0].message.content.strip('"\'')
        except OpenAIError as e:
            print(f"Error: {e}. Retrying...")
            time.sleep(5)
    raise Exception(f"Failed to process after {retries} retries.")

def create_new_description(row):
    """Generates a new SEO-optimized description using OpenAI."""
    prompt = f"Gere uma descrição otimizada para SEO sobre o vídeo com título: {row['title']}. Retorne apenas a descrição."
    return generate_ai_response(prompt) + f"\n\n{row['description']}"

def suggest_tags(row):
    """Generates SEO-optimized tags using OpenAI."""
    prompt = f"Otimize 20 tags para o vídeo '{row['title']}'. Título: {row['title']}, Descrição: {row['new_description']}, Tags atuais: {row['tags']}."
    return generate_ai_response(prompt)

def clean_and_fix_tags(tags):
    """Validates and trims tags to fit YouTube's limits."""
    tags = [tag.strip().replace(".", "") for tag in tags.split(",") if len(tag.strip()) <= 30]
    while len(", ".join(tags)) > 500:
        tags.pop(random.randint(0, len(tags) - 1))
    return ", ".join(tags)

if __name__ == "__main__":
    # Load YouTube API key
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

    # Define YouTube channels
    CHANNELS = {"xadrezBrasil": {"id": "UC5K-TQsItHnNLjqYf8A3CTw", "country": "Brazil"}}

    # Extract data
    df = extract_data(CHANNELS, YOUTUBE_API_KEY, CATEGORY_DICT)
    df = df[df["description"].str.contains(IDENTIFIER) == False]

    if not df.empty:
        df["new_description"] = df.apply(create_new_description, axis=1)
        df["new_suggested_tags"] = df.apply(suggest_tags, axis=1)
        df["new_suggested_tags"] = df["new_suggested_tags"].apply(clean_and_fix_tags)

        # Update YouTube
        failed_updates = update_youtube_videos(df)
        if not failed_updates.empty:
            failed_updates.to_excel("youtube_failed_videos.xlsx", index=False)

    print("YouTube video update process completed successfully!")
