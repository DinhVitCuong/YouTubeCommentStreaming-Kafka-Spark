import os
import time
import json
from kafka import KafkaProducer
from googleapiclient.discovery import build

# —————— Config ——————
VIDEO_ID   = video_id
API_KEY    = your_youtube_api_key
BOOTSTRAP  = "localhost:9092"
TOPIC      = "YouTubeComments"
# ————————————————

# init YouTube client
youtube = build("youtube", "v3", developerKey=API_KEY)

# init Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)
seen_ids = set()

def fetch_and_send(next_page_token=None):
    resp = youtube.commentThreads().list(
        part="snippet",
        videoId=VIDEO_ID,
        pageToken=next_page_token,
        maxResults=50,
        textFormat="plainText"
    ).execute()
    for item in resp.get("items", []):
        cid = item["id"]
        if cid in seen_ids:
            continue
        seen_ids.add(cid)

        c = item["snippet"]["topLevelComment"]["snippet"]
        rec = {
            "comment_id": cid,
            "user":       c["authorDisplayName"],
            "text":       c["textDisplay"],
            "timestamp":  c["publishedAt"]
        }

        producer.send(TOPIC, value=rec)
        print(f"→ Sent to Kafka: {rec}")

        time.sleep(5)
    producer.flush()

    return resp.get("nextPageToken")

if __name__ == "__main__":
    token = None
    while True:
        token = fetch_and_send(next_page_token=token)
        if not token:
            break
        time.sleep(10)  
