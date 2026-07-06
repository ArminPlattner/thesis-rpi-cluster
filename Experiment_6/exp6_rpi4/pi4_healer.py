#!/usr/bin/env python3
import json
import subprocess
import time
from confluent_kafka import Consumer, Producer

# Ground Truth Oracle
RULE_MAPPING = {
    ("Security", "State Mismanagement"): "R1",
    ("Configuration Data", "Variable Misreference"): "R2",
    ("Service", "Dependency-related Faults"): "R3",
    ("Service", "State Mismanagement"): "R4",
    ("Idempotency", "State Mismanagement"): "R5",
    ("Configuration Data", "State Mismanagement"): "R6",
    ("Idempotency", "Incorrect Task Iteration Logic"): "R7"
}

def run_ansible(rule):
    # -i ../hosts.ini ensures Ansible finds the inventory from the exp_7 folder
    cmd = [
        "ansible-playbook", 
        "-i", "../hosts.ini", 
        "remediation_catalog.yml", 
        "--extra-vars", f"target_rule={rule}"
    ]
    print(f"Executing: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def main():
    consumer = Consumer({
        "bootstrap.servers": "10.42.0.184:32709", 
        "group.id": "pi4-healer"
    })
    producer = Producer({
        "bootstrap.servers": "10.42.0.184:32709"
    })
    
    consumer.subscribe(["ansible-classified"])
    print("Pi4 Healer Online and emitting ACKs...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): 
            continue
            
        data = json.loads(msg.value().decode("utf-8"))

        # Resolve Rule
        # Note: we establish the following lines as a fallback mechanism to ensure, even with lower confidence, that the correct rule is triggered. In this way, we can measure the success rate of classifications and if the correct rules were triggered purely from the classification or if there were outages. 
        rule = data.get("assigned_rule") or RULE_MAPPING.get(
            (data.get("iac_category"), data.get("fault_category")), "MANUAL"
        )

        if data.get("is_healable") and rule != "MANUAL":
            print(f"[ACTION] Triggering Remediation: {rule} for {data.get('msg_id')}")
            try:
                run_ansible(rule)
                
                # Send Success ACK back to ingestion engine
                ack_payload = {
                    "msg_id": data["msg_id"], 
                    "status": "OK", 
                    "rule": rule
                }
                producer.produce("healer-ack", json.dumps(ack_payload).encode("utf-8"))
                producer.flush()
                print(f"[ACK SENT] Success for {data.get('msg_id')}")
                
            except Exception as e:
                print(f"Remediation Failed: {e}")
                # Send Failure ACK
                fail_payload = {
                    "msg_id": data["msg_id"], 
                    "status": "FAIL", 
                    "error": str(e)
                }
                producer.produce("healer-ack", json.dumps(fail_payload).encode("utf-8"))
                producer.flush()
        else:
            print(f"[ESCALATE] Manual intervention required: {data.get('msg_id')}")

if __name__ == "__main__":
    main()
