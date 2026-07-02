#!/usr/bin/env python3
import os
import time
import json
import pandas as pd
from confluent_kafka import Producer

RAW_DATA_PATH = "ansible_full_stream_new.csv"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
OUT_TOPIC = "ansible-raw-logs"

TOTAL_EXPECTED = 5500

def load_dataset(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing dataset file: {path}")
    df = pd.read_csv(path)

    required = {"log", "iac_category", "fault_category"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Dataset missing required columns: {sorted(missing)}")

    return df

def main():
    print(f"Loading dataset: {RAW_DATA_PATH}")
    df = load_dataset(RAW_DATA_PATH)

    # DISTRIBUTION AUDIT 
    print("\n" + "="*50)
    print("PRE-INGESTION DISTRIBUTION AUDIT")
    iac_dist = df["iac_category"].value_counts(normalize=True) * 100
    for cat, pct in iac_dist.items():
        print(f"{cat:<20}: {pct:>6.2f}%")
    print("="*50 + "\n")

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 5,
        "compression.type": "lz4",
    })

    total_rows = len(df)
    print(f"Ingesting {total_rows} logs into Kafka topic [{OUT_TOPIC}]")

    start_time = time.time()

    for idx, row in df.iterrows():
        payload = {
            "msg_id": f"TX-5000-{idx+1:04d}",
            "timestamp": time.time(),
            "raw_log": str(row["log"]),
            "true_iac": str(row["iac_category"]),
            "true_fault": str(row["fault_category"]),
        }

        producer.produce(OUT_TOPIC, json.dumps(payload).encode("utf-8"))

        if idx % 500 == 0:
            producer.poll(0)
    
    producer.flush()
    duration = time.time() - start_time

    n_total = len(df)
    n_success = len(df[df["iac_category"] == "SUCCESS"])
    n_anomalous = n_total - n_success

    print(f"\nIngestion completed in {duration:.2f} seconds")
    print(f"Throughput: {total_rows / duration:.2f} messages/sec")
    print(f"Total logs sent: {total_rows}")
    print(f"Composition: {n_success} successful + {n_anomalous} anomalous = {n_total} total")

if __name__ == "__main__":
    main()
