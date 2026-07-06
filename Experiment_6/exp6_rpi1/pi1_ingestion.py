#!/usr/bin/env python3
"""
Pi1 ingestion script with deterministic rule-based anomaly selection.
"""

import argparse
import json
import os
import random
import subprocess
import time
import pandas as pd
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP = "10.42.0.184:32709"
RAW_TOPIC = "ansible-raw-logs"
CONFIRM_TOPIC = "healer-ack"
INJECT_PLAYBOOK = "./fault_inject.yml"
INVENTORY = "./hosts.ini"
TEST_POOL_CSV = "../ansible_anomaly_pool_new_test.csv"

RULE_PROFILE = {
    "R1": {"desc": "SSH brute-force mitigation", "iac": "Security", "fault": "State Mismanagement", "inject_tag": "R1"},
    "R2": {"desc": "Config drift rollback", "iac": "Configuration Data", "fault": "Variable Misreference", "inject_tag": "R2"},
    "R3": {"desc": "Service controlled restart", "iac": "Service", "fault": "Dependency-related Faults", "inject_tag": "R3"},
    "R4": {"desc": "High CPU scale-out", "iac": "Service", "fault": "State Mismanagement", "inject_tag": "R4"},
    "R5": {"desc": "Critical CPU cordon", "iac": "Idempotency", "fault": "State Mismanagement", "inject_tag": "R5"},
    "R6": {"desc": "Low disk prune", "iac": "Configuration Data", "fault": "State Mismanagement", "inject_tag": "R6"},
    "R7": {"desc": "Critical disk evict", "iac": "Idempotency", "fault": "Incorrect Task Iteration Logic", "inject_tag": "R7"}
}

def make_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 0, "acks": 1})

def make_ack_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"pi1-ack-listener-{int(time.time())}",
        "auto.offset.reset": "latest",
    })

def load_test_pool(path):
    df = pd.read_csv(path)
    if "iac_category" in df.columns:
        df = df[df["iac_category"] != "SUCCESS"].reset_index(drop=True)
    return df

def select_log_for_rule(df, rule_id):
    profile = RULE_PROFILE[rule_id]
    # Deterministic filtering based on ground truth
    candidates = df[
        (df["iac_category"] == profile["iac"]) & 
        (df["fault_category"] == profile["fault"])
    ]
    if candidates.empty:
        candidates = df[df["iac_category"] == profile["iac"]]
        
    return candidates.sample(n=1, random_state=42).iloc[0]

def inject_fault(tag):
    print(f"  [INJECT] {tag}")
    result = subprocess.run(
        ["ansible-playbook", "-i", INVENTORY, INJECT_PLAYBOOK, "--tags", tag],
        capture_output=True, text=True,
    )
    return result.returncode == 0

def run_experiment_6(df):
    print("\n" + "=" * 70)
    print("EXPERIMENT 6 — Deterministic Oracle Run")
    print("=" * 70)

    producer = make_producer()
    ack_cons = make_ack_consumer()
    ack_cons.subscribe([CONFIRM_TOPIC])

    results = {rule: [] for rule in RULE_PROFILE}

    for rule_id, profile in RULE_PROFILE.items():
        print(f"\nTargeting {rule_id}: {profile['desc']}")
        inject_fault(profile["inject_tag"])

        row = select_log_for_rule(df, rule_id)
        t_send = time.time_ns()
        msg_id = f"pi1-exp6-{rule_id}-{t_send}"
        
        # Injecting classification metadata as requested
        payload = {
            "raw_log": row["log"],
            "iac_category": row["iac_category"],
            "fault_category": row["fault_category"],
            "assigned_rule": rule_id, 
            "msg_id": msg_id,
            "t_send_ns": t_send
        }

        print(f"  Sending {rule_id} log -> msg_id={msg_id}")
        producer.produce(RAW_TOPIC, json.dumps(payload).encode("utf-8"))
        producer.flush()

        # Await Ack
        deadline = time.time() + 120
        while time.time() < deadline:
            ack_msg = ack_cons.poll(0.5)
            if ack_msg and not ack_msg.error():
                ack = json.loads(ack_msg.value().decode("utf-8"))
                if ack.get("msg_id") == msg_id:
                    t_ack = time.time_ns()
                    mttr = (t_ack - t_send) / 1_000_000_000
                    print(f"  ACK received for {rule_id} | MTTR={mttr:.3f}s")
                    results[rule_id].append(mttr)
                    break
        time.sleep(2)
    ack_cons.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", type=int, choices=[6, 7], required=True)
    parser.add_argument("--csv", type=str, default=TEST_POOL_CSV)
    args = parser.parse_args()

    df = load_test_pool(args.csv)
    if args.experiment == 6:
        run_experiment_6(df)

if __name__ == "__main__":
    main()
