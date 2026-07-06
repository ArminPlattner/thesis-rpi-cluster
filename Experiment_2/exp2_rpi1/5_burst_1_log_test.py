import time
import json
import pandas as pd
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "10.42.0.184:32709"
TOPIC = "ansible-raw-logs"
CSV_FILE = "/home/ubuntu/ansible_anomaly_pool_new_test.csv"

print(f"Loading stratified Phase 6 test dataset from {CSV_FILE}...")
df = pd.read_csv(CSV_FILE)

p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def run_intermittent_test(cycles=5, interval_seconds=12):
    print(f"Starting intermittent evaluation loop: {cycles} logs, {interval_seconds}s intervals.")

    for cycle in range(1, cycles + 1):
        # Sample directly from the stratified test dataset
        row = df.sample(n=1).iloc[0]
        log_line = str(row["log"])
        true_iac = row.get("iac_category", "unknown")
        true_fault = row.get("fault_category", "unknown")

        msg_id = f"pi1-v6-{int(time.time_ns())}-{cycle}"

        payload = {
            "msg_id": msg_id,
            "raw_log": log_line,
            "true_iac_category": true_iac,
            "true_fault_category": true_fault,
            "t_send_ns": time.time_ns()
        }

        print(f"[{cycle}/{cycles}] Dispatching {msg_id}")
        print(f"   ↳ GT IaC: {true_iac} | GT Fault: {true_fault}")
        
        p.produce(TOPIC, json.dumps(payload).encode("utf-8"))
        p.flush()

        if cycle < cycles:
            print(f"💤 Sleeping for {interval_seconds} seconds...")
            time.sleep(interval_seconds)

    print("Intermittent evaluation sequence completed successfully.")

if __name__ == "__main__":
    run_intermittent_test(cycles=5, interval_seconds=12)
