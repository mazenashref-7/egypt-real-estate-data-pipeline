import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
TOPIC         = os.getenv("TOPIC", "listings_raw")
CSV_PATH      = os.getenv("CSV_PATH", "listings.csv")
STATE_FILE    = os.getenv("STATE_FILE", "state/producer_offset.txt")
MSGS_PER_SEC  = float(os.getenv("MSGS_PER_SEC", "20"))
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "20"))
END_AT        = int(os.getenv("END_AT", "0"))     

SLEEP_SEC = BATCH_SIZE / MSGS_PER_SEC


def read_state():
    try:
        with open(STATE_FILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0


def save_state(n):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        f.write(str(n))
    os.replace(tmp, STATE_FILE)          # atomic write


# ---------- connect to Kafka (retry until it is ready) ----------
def connect():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in 5s...", flush=True)
            time.sleep(5)


producer = connect()
start = read_state()
print(f"Starting from row {start}", flush=True)

row_no = 0      # rows read so far (global counter)
sent = 0        # rows sent in this run

for chunk in pd.read_csv(CSV_PATH, dtype=str, chunksize=1000):
    # skip whole chunks that were already sent
    if row_no + len(chunk) <= start:
        row_no += len(chunk)
        continue

    chunk = chunk.astype(object).where(chunk.notna(), None)
    for row in chunk.to_dict(orient="records"):
        row_no += 1
        if row_no <= start:
            continue
        if END_AT and row_no > END_AT:
            break

        producer.send(TOPIC, value=row)
        sent += 1

        if sent % BATCH_SIZE == 0:
            producer.flush()
            save_state(row_no)
            print(f"Sent {sent} messages (row {row_no})", flush=True)
            time.sleep(SLEEP_SEC)

    if END_AT and row_no > END_AT:
        break

producer.flush()
save_state(row_no if not END_AT else min(row_no, END_AT))
print(f"Done. Sent {sent} messages in this run.", flush=True)