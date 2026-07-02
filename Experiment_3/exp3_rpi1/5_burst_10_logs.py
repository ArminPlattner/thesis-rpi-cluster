import pandas as pd
import json
import time
import numpy as np
from confluent_kafka import Producer

# CONFIG
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
TOPIC = "ansible-raw-logs"
CSV_FILE = "../ansible_anomaly_pool_new_test.csv"

# Gang of 8 Taxonomy Weights
CATEGORIES = [
    'Conditional', 'Configuration Data', 'Dependency',
    'Documentation', 'Idempotency', 'Security', 'Service', 'Syntax'
]
WEIGHTS = [0.3, 9.5, 1.8, 1.7, 0.1, 0.1, 1.4, 2.8]
WEIGHTS = np.array(WEIGHTS) / np.sum(WEIGHTS)

print(f"Loading dataset from {CSV_FILE}...")
df = pd.read_csv(CSV_FILE)
data_by_cat = {cat: df[df['iac_category'] == cat] for cat in CATEGORIES}

sent_counts = {cat: 0 for cat in CATEGORIES}
p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def run_burst_test():
    print("Starting 5 Weighted Bursts of 10 Logs (Total 50 Logs)...")
    
    # 1. Stratified Injection: Ensure at least one of each category
    # This prevents the Security category (or others) from being skipped
    mandatory_samples = [data_by_cat[cat].sample(1).iloc[0] for cat in CATEGORIES]
    
    # 2. Setup the full batch (8 mandatory + 42 weighted random)
    batch = mandatory_samples.copy()
    for _ in range(42):
        chosen_cat = np.random.choice(CATEGORIES, p=WEIGHTS)
        batch.append(data_by_cat[chosen_cat].sample(1).iloc[0])
    
    # Shuffle to ensure mandatory logs are randomized
    np.random.shuffle(batch)

    # 3. Execution Loops
    for burst in range(1, 6):
        print(f"\n{'='*60}\nBURST {burst}/5 — STRATIFIED SAMPLING ACTIVE\n{'='*60}")

        for sub_idx in range(10):
            # Pop the next log from our prepared batch
            row = batch.pop(0)
            sent_counts[row['iac_category']] += 1
            
            log_text = row['log']
            gt_iac = row['iac_category']
            gt_fault = row['fault_category']

            t_send = time.time_ns()
            msg_id = f"pi1-{t_send}-b{burst}-l{sub_idx}"

            # Routing Logic Print
            manual_faults = ["Typos", "Variable Misreference", "Incorrect Task Iteration Logic"]
            should_heal = "HEALABLE (ansible-classified)"
            if gt_iac in ["Syntax", "Documentation"] or gt_fault in manual_faults:
                should_heal = "MANUAL (ansible-manual-intervention)"

            print(f"[Cat: {gt_iac}] Msg ID: {msg_id}")
            print(f"   | Path: Gatekeeper TinyBERT {should_heal}")
            preview = log_text.strip().splitlines()[0][:85] if log_text.strip() else "EMPTY LOG"
            print(f"   | Raw Snippet:   {preview}...")
            print(f"   " + "-"*45)

            # Payload
            payload = {
                "raw_log": log_text,
                "timestamp": time.time(),
                "t_send_ns": t_send,
                "msg_id": msg_id,
                "true_iac_category": gt_iac,
                "true_fault_category": gt_fault
            }
            p.produce(TOPIC, json.dumps(payload).encode('utf-8'))
        
        p.flush()
        if burst < 5:
            print(f"\nPausing 12 seconds for pipeline execution...")
            time.sleep(12)

    # Final Summary
    print("\n" + "="*60)
    print("All bursts complete. Actual Distribution Summary:")
    print(f"{'Category':<20} | {'Count':<6} | {'Percentage':<10}")
    print("-" * 45)
    for cat, count in sent_counts.items():
        print(f"{cat:<20} | {count:<6} | {(count/50)*100:.1f}%")
    print("="*60)

if __name__ == "__main__":
    run_burst_test()
