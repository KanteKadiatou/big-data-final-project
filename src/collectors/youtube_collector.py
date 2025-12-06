import os
import json
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled

# --- Configuration ---
API_KEY = "AIzaSyDRXJDbbR3iX0YOF4Y6YKYg86ufqckyqu0"  # Remplace par ta clé
CHANNEL_HANDLES = [
    "casioeducationfrance",
    "notabenemovies",
    "lageozone",
    "clementvousenseigne",
    "khanacademy",
    "bbclearningenglish",
    "machinelearnia"
]
VIDEOS_PER_CHANNEL = 5
OUTPUT_FILE = "raw_videoyoutube.json"

# --- Initialisation YouTube API ---
youtube = build("youtube", "v3", developerKey=API_KEY)

def get_channel_id(handle):
    res = youtube.channels().list(forUsername=handle, part="id").execute()
    if res.get("items"):
        return res["items"][0]["id"]
    # Fallback: handle = @handle
    res = youtube.search().list(q=handle, type="channel", part="id", maxResults=1).execute()
    if res.get("items"):
        return res["items"][0]["id"]["channelId"]
    return None

def get_videos(channel_id, max_results=5):
    res = youtube.search().list(channelId=channel_id, part="id", type="video",
                                order="date", maxResults=max_results).execute()
    return [item["id"]["videoId"] for item in res.get("items", [])]

def get_video_metadata(video_id):
    res = youtube.videos().list(id=video_id, part="snippet,contentDetails,statistics").execute()
    if not res.get("items"):
        return None
    item = res["items"][0]
    meta = {
        "video_id": video_id,
        "title": item["snippet"]["title"],
        "description": item["snippet"]["description"],
        "tags": item["snippet"].get("tags", []),
        "channel": item["snippet"]["channelTitle"],
        "published_at": item["snippet"]["publishedAt"],
        "duration": item["contentDetails"]["duration"],
        "views": item["statistics"].get("viewCount", "0"),
        "likes": item["statistics"].get("likeCount", "0"),
        "comments": item["statistics"].get("commentCount", "0")
    }
    return meta

def get_transcript(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=["fr", "en"])
        return " ".join([t["text"] for t in transcript_list])
    except (TranscriptsDisabled, Exception):
        return None

# --- Collecte ---
all_videos = []

for handle in CHANNEL_HANDLES:
    print(f"Collecte des vidéos pour la chaîne: {handle}")
    channel_id = get_channel_id(handle)
    if not channel_id:
        print(f"⚠️ Impossible de récupérer le channelId pour {handle}")
        continue
    video_ids = get_videos(channel_id, VIDEOS_PER_CHANNEL)
    for vid in video_ids:
        meta = get_video_metadata(vid)
        transcript = get_transcript(vid)
        all_videos.append({
            "metadata": meta,
            "transcript": transcript
        })

# --- Sauvegarde JSON ---
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(all_videos, f, ensure_ascii=False, indent=4)

print(f"✅ Collecte terminée, {len(all_videos)} vidéos enregistrées dans {OUTPUT_FILE}")
