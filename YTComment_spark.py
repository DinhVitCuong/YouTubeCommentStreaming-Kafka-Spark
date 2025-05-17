import os, glob, re, time, json
from threading import Thread

from googleapiclient.discovery import build
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
from transformers import pipeline
import torch

VIDEO_ID   = video_id_hehe
API_KEY    = your_youtube_api_key
INPUT_DIR = your_input_dir
CHECKPOINT_DIR = your_checkpoint_dir
FETCH_INTERVAL = 30  # seconds

# --- 0. clear out old files ---
os.makedirs(INPUT_DIR, exist_ok=True)
for f in glob.glob(f"{INPUT_DIR}/*.json"):
    os.remove(f)
print(f"[Init] Cleared {INPUT_DIR}")
seen_ids = set()

# --- 1. SparkSession with local[*] ---
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("StreamingCommentsSentiment") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("[Spark] Session ready")

# --- 2. Define schema & readStream---
schema = (StructType()
          .add("comment_id", StringType())
          .add("user",       StringType())
          .add("text",       StringType())
          .add("timestamp",  StringType())
         )

raw = (spark.readStream
       .schema(schema)
       .option("maxFilesPerTrigger", 5)
       .json(INPUT_DIR)
      )

# --- 3. Simple cleaning (SQL functions) ---
cleaned = raw.withColumn(
    "text_clean",
    lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", ""))
)
# --- 4. Build the YouTube fetcher ---
youtube = build("youtube", "v3", developerKey=API_KEY)
def fetch_and_dump(next_page_token=None):
    resp = youtube.commentThreads().list(
        part="snippet",
        videoId=VIDEO_ID,
        pageToken=next_page_token,
        maxResults=50,
        textFormat="plainText"
    ).execute()
    os.makedirs(INPUT_DIR, exist_ok=True)

    for item in resp.get("items", []):
        cid = item["id"]
        if cid in seen_ids:
            continue
        seen_ids.add(cid)

        c = item["snippet"]["topLevelComment"]["snippet"]
        rec = {
            "comment_id": cid,
            "user":        c["authorDisplayName"],
            "text":        c["textDisplay"],
            "timestamp":   c["publishedAt"]
        }
        fn = f"{cid}_{int(time.time())}.json"
        with open(os.path.join(INPUT_DIR, fn), "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False)

    return resp.get("nextPageToken")

def start_fetcher():
    token = None
    print("[Fetcher] starting")
    while True:
        try:
            token = fetch_and_dump(token)
            print(f"[Fetcher] fetched (next={token})")
        except Exception as e:
            print("[Fetcher] error:", e)
        time.sleep(FETCH_INTERVAL)

# --- 5. Driver‐side sentiment + print via foreachBatch ---
device = 0 if torch.cuda.is_available() else -1
sentiment_pipe = pipeline("sentiment-analysis", device=device)
print(f"[Init] sentiment pipeline on {'GPU' if device>=0 else 'CPU'}")

def process_and_print(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return
    pdf = batch_df.select("comment_id","user","text","text_clean","timestamp") \
                  .toPandas()
    results = sentiment_pipe(list(pdf["text_clean"].str[:512]))
    pdf["sentiment_label"] = [r["label"] for r in results]
    pdf["sentiment_score"] = [r["score"] for r in results]
    print("\n=== Batch", epoch_id, "===")
    print(pdf[["comment_id","user","text","sentiment_label","sentiment_score","timestamp"]]
          .to_string(index=False))
    print()

query = (cleaned
         .writeStream
         .outputMode("append")
         .foreachBatch(process_and_print)
         .option("checkpointLocation", CHECKPOINT_DIR)
         .trigger(processingTime="10 seconds")
         .start()
        )

# --- 6. Kick off fetcher thread, then wait ---
fetch_thread = Thread(target=start_fetcher, daemon=True)
fetch_thread.start()

query.awaitTermination()