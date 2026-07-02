import sys
import os
import time
import json
import argparse
import pandas as pd
import numpy as np
from confluent_kafka import Producer

# CONFIGURATION CONSTANTS
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
TOPIC = "ansible-raw-logs"
CSV_FILE = "ansible_anomaly_pool_new_test.csv"

# Gang of 8 Taxonomy Weights
CATEGORIES = [
    'Conditional', 'Configuration Data', 'Dependency',
    'Documentation', 'Idempotency', 'Security', 'Service', 'Syntax'
]
WEIGHTS = [0.3, 9.5, 1.8, 1.7, 0.1, 0.1, 1.4, 2.8]
WEIGHTS = np.array(WEIGHTS) / np.sum(WEIGHTS)

def run_stress_test(target_volume, intra_log_delay_ms):
    # 1. Resolve and Load the Evaluation Dataset
    target_csv = CSV_FILE
    if not os.path.exists(target_csv):
        fallback = os.path.join("..", CSV_FILE)
        if os.path.exists(fallback):
            target_csv = fallback
        else:
            print(f"❌ Error: Cannot find dataset '{CSV_FILE}'")
            return

    print(f"Loading dataset from {target_csv}...")
    df = pd.read_csv(target_csv)
    data_by_cat = {cat: df[df['iac_category'] == cat] for cat in CATEGORIES}

    # 2. Initialize the Kafka Producer
    try:
        p = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP,
            'linger.ms': 0,
            'acks': 1
        })
    except Exception as e:
        print(f"Failed to initialize Confluent Kafka Producer: {e}")
        return

    print(f"\nINITIATING WEIGHTED STRESS RUN: {target_volume} LOGS")
    print(f"Intra-Log Delay: {intra_log_delay_ms} ms")
    print("="*70)

    start_time_ns = time.perf_counter_ns()
    start_wall_time = time.strftime('%H:%M:%S')
    
    injected_categories = []
    delay_sec = intra_log_delay_ms / 1000.0

    # 3. High-Precision Injection Loop
    for current_count in range(1, target_volume + 1):
        chosen_cat = np.random.choice(CATEGORIES, p=WEIGHTS)
        injected_categories.append(chosen_cat)
        
        subset = data_by_cat[chosen_cat]
        row = subset.sample(1).iloc[0] if not subset.empty else df.sample(1).iloc[0]

        log_text = str(row['log'])
        true_iac = str(row.get('iac_category', 'Unknown'))
        true_fault = str(row.get('fault_category', 'Unknown'))

        t_send_sec = time.time()
        msg_id = f"pi1-{int(t_send_sec * 1e9)}-vol{target_volume}-{current_count}"

        payload = {
            "raw_log": log_text,
            "timestamp": t_send_sec,
            "msg_id": msg_id,
            "true_iac_category": true_iac,
            "true_fault_category": true_fault
        }

        p.produce(TOPIC, json.dumps(payload).encode('utf-8'))
        p.poll(0)

        if delay_sec > 0:
            time.sleep(delay_sec)

        if current_count == 1 or current_count == target_volume or current_count % max(1, target_volume // 5) == 0:
            print(f"Progress [{current_count}/{target_volume}] -> Injected: {chosen_cat}")

    # 4. Flush and Report
    print(f"\nOperational queue filled. Flushing remaining data...")
    p.flush()
    
    total_duration = (time.perf_counter_ns() - start_time_ns) / 1e9
    throughput = target_volume / total_duration

    # 5. Audit Distribution (Matched to your preferred format)
    print("\n" + "="*60)
    print("All logs sent. Actual Distribution Summary:")
    print(f"{'Category':<20} | {'Count':<6} | {'Percentage':<10}")
    print("-" * 45)
    
    audit_series = pd.Series(injected_categories).value_counts()
    for cat in CATEGORIES:
        count = audit_series.get(cat, 0)
        percentage = (count / target_volume) * 100
        print(f"{cat:<20} | {count:<6} | {percentage:.1f}%")
    print("="*60)

    # 6. Telemetry Report
    print("\nLIVE LOG EMISSION & BUFFER FLUSH TELEMETRY REPORT")
    print("="*70)
    print(f" 🔹 Ingestion Job Started      : {start_wall_time}")
    print(f" 🔹 Ingestion Job Finished     : {time.strftime('%H:%M:%S')}")
    print(f" 🔹 Total Volume Transmitted   : {target_volume} logs")
    print(f" 🔹 Local Injection Throughput : {throughput:.2f} logs/second")
    print("="*70)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Experiment 4 Stream Ingestion Engine")
    parser.add_argument("volume", type=int, help="Target log volume")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay in ms")
    args = parser.parse_args()

    run_stress_test(args.volume, args.delay)
