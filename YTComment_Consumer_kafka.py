import json
import time
from kafka import KafkaConsumer
from transformers import pipeline
import torch
import re

# ——— CONFIG ———
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC       = "YouTubeComments"
GROUP_ID          = "sentiment-consumer"
MAX_LENGTH        = 512

# ——— Sentiment Pipeline Init ———
device = 0 if torch.cuda.is_available() else -1
sentiment_pipe = pipeline("sentiment-analysis", device=device)
print(f"[Init] Sentiment pipeline on {'GPU' if device >= 0 else 'CPU'}")

# ——— Text Preprocess ———
def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9\s]", "", text).lower()

# ——— Kafka Consumer Init ———
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[INFO] Listening for Kafka messages...")

# ——— Process messages ———
for message in consumer:
    data = message.value

    if not all(k in data for k in ["comment_id", "user", "text", "timestamp"]):
        print("[WARN] Missing fields in message:", data)
        continue

    text_raw = data["text"]
    text_clean = clean_text(text_raw)
    if not text_clean.strip():
        continue

    try:
        sentiment = sentiment_pipe(text_clean[:MAX_LENGTH])[0]
    except Exception as e:
        print(f"[ERROR] Failed to process comment: {e}")
        continue

    print("\n==============================")
    print(f"User     : {data['user']}")
    print(f"Comment  : {text_raw}")
    print(f"Sentiment: {sentiment['label']} (Score: {sentiment['score']:.3f})")
    print(f"Time     : {data['timestamp']}")