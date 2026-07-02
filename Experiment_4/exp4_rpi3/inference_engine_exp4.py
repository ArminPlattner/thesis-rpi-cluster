#!/usr/bin/env python3
import os
import sys
import json
import time
import pickle
import argparse
import numpy as np
from transformers import AutoTokenizer
from confluent_kafka import Consumer, Producer
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction
import onnxruntime as ort
import signal

# CONFIGURATION CONSTANTS
MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_V6_Quantized_DRAIN"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
CONFIDENCE_THRESHOLD = 0.90
IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"

CPU_THREADS = 2
MAX_LEN = 256

# ARGUMENT PARSING 
parser = argparse.ArgumentParser(description="Experiment 4 (DRAIN + INT8) Automated Inference Profiler Loop")
parser.add_argument("expected_volume", type=int, help="Target threshold volume cutoff limit (>0)")
args = parser.parse_args()
TOTAL_EXPECTED_ANOMALIES = args.expected_volume

# TELEMETRY TRACKER INFRASTRUCTURE
metrics = {
    "start_wall_time": None,
    "end_wall_time": None,
    "inference_latencies_ms": [],
    "template_latencies_ms": [],
    "pure_processing_latencies_ms": [],
    "e2e_pipeline_latencies_ms": [],
    "correct_iac_classifications": 0,
    "correct_fault_classifications": 0,
    "processed_count": 0,
    "rule_distribution": {
        "R12: Master Node Recovery": 0,
        "LOG_UNRECOGNIZED (Safety Gate)": 0,
        "R3: Controlled Service Restart": 0,
        "R2: Rollback to last-known-good config": 0,
        "R15: Repair File Permissions": 0,
        "Manual Escalations": 0
    }
}

# Graceful shutdown flag
_graceful_stop = False
def _signal_handler(sig, frame):
    global _graceful_stop
    _graceful_stop = True
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# ENVIRONMENT & ASSET VERIFICATION 
if not os.path.exists(MODEL_PATH):
    print(f"Error: Model asset path directory not found: {MODEL_PATH}")
    sys.exit(1)

print(f"Loading quantized ONNX + Drain assets from {MODEL_PATH}...")

# Labels
with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f:
    labels_data = json.load(f)
    iac_labels = labels_data["iac_labels"]
    fault_labels = labels_data["fault_labels"]

# DRAIN PARSER SETUP (load or create drain_state.bin)
config = TemplateMinerConfig()
config.masking_instructions = [
    MaskingInstruction(r"\*+", "STARS"),
    MaskingInstruction(r"(?<=TASK \[).+?(?=\])", "TASK_NAME"),
    MaskingInstruction(r"\b(?:[A-Za-z0-9_-]+\.)+[A-Za-z]{2,}\b", "HOST"),
]
config.drain_depth = 6
config.drain_sim_th = 0.7

state_bin = os.path.join(MODEL_PATH, "drain_state.bin")
if os.path.exists(state_bin):
    print(f"Loading existing Drain state from {state_bin}...")
    with open(state_bin, "rb") as f:
        template_miner = pickle.load(f)
else:
    print(f"No drain_state.bin found; creating new TemplateMiner (will persist on shutdown).")
    template_miner = TemplateMiner(config=config)

# ONNX ENGINE SETUP
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
onnx_files = [f for f in os.listdir(MODEL_PATH) if f.endswith(".onnx")]
if not onnx_files:
    raise FileNotFoundError(f"No .onnx model found in {MODEL_PATH}")
ONNX_PATH = os.path.join(MODEL_PATH, onnx_files[0])

sess_options = ort.SessionOptions()
sess_options.intra_op_num_threads = CPU_THREADS
sess_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
session = ort.InferenceSession(ONNX_PATH, sess_options, providers=["CPUExecutionProvider"])
print(f"Quantized INT8 ONNX + Drain engine initialized successfully.")

# UTILITY HELPER FUNCTIONS 
def apply_drain_template(raw_log):
    """Run Drain on raw_log and return the template string."""
    flattened = str(raw_log).strip()
    match = template_miner.match(flattened)
    if match:
        return match.get_template()
    else:
        return template_miner.add_log_message(flattened)["template_mined"]

def get_sub_system(text):
    t = text.lower()
    if any(k in t for k in ["mysql", "mariadb", "postgres", "sqlite", "database"]):
        return "Storage System"
    if any(k in t for k in ["port", "tcp", "bind", "address", "dhcp", "ip", "ssh"]):
        return "Network Setup"
    if any(k in t for k in ["permission", "chmod", "chown", "directory", "path"]):
        return "File System"
    return "General Infrastructure"

def softmax(x):
    x = x - np.max(x, axis=-1, keepdims=True)
    ex = np.exp(x)
    return ex / ex.sum(axis=-1, keepdims=True)

def predict_with_safety_gate(raw_log, model_input):
    infer_start_ns = time.perf_counter_ns()

    inputs = tokenizer(
        model_input,
        truncation=True,
        padding="max_length",
        max_length=MAX_LEN,
        return_tensors="np"
    )

    onnx_inputs = {
        "input_ids": inputs["input_ids"].astype(np.int64),
        "attention_mask": inputs["attention_mask"].astype(np.int64),
    }

    outputs = session.run(None, onnx_inputs)
    probs_iac = softmax(outputs[0][0])
    probs_fault = softmax(outputs[1][0])

    inference_ms = (time.perf_counter_ns() - infer_start_ns) / 1e6

    iac_idx = int(np.argmax(probs_iac))
    fault_idx = int(np.argmax(probs_fault))

    iac_cat = iac_labels[iac_idx]
    iac_conf = float(probs_iac[iac_idx])
    fault_cat = fault_labels[fault_idx]
    fault_conf = float(probs_fault[fault_idx])

    is_healable = True
    reason = "High confidence autonomic match"
    manual_faults = ["Typos", "Variable Misreference", "Incorrect Task Iteration Logic"]

    if iac_cat in ["Syntax", "Documentation"] or fault_cat in manual_faults:
        is_healable = False
        reason = f"Manual Intervention Required: {fault_cat}"
    elif iac_conf < CONFIDENCE_THRESHOLD:
        is_healable = False
        reason = f"Uncertain Classification (Conf: {iac_conf:.2f})"

    return {
        "raw_log": raw_log,
        "template_text": model_input,
        "iac_category": iac_cat,
        "iac_confidence": iac_conf,
        "fault_category": fault_cat,
        "fault_confidence": fault_conf,
        "sub_system": get_sub_system(model_input),
        "is_healable": is_healable,
        "escalation_reason": reason,
        "inference_ms": inference_ms,
    }

def print_thesis_report():
    total_pipeline_time = metrics["end_wall_time"] - metrics["start_wall_time"] if metrics["end_wall_time"] and metrics["start_wall_time"] else 0.0
    mean_inference = np.mean(metrics["inference_latencies_ms"]) if metrics["inference_latencies_ms"] else 0.0
    mean_template = np.mean(metrics["template_latencies_ms"]) if metrics["template_latencies_ms"] else 0.0
    throughput = metrics["processed_count"] / total_pipeline_time if total_pipeline_time > 0 else 0.0

    success_rate_iac = (
        (metrics["correct_iac_classifications"] / metrics["processed_count"]) * 100
        if metrics["processed_count"] > 0 else 0.0
    )
    success_rate_fault = (
        (metrics["correct_fault_classifications"] / metrics["processed_count"]) * 100
        if metrics["processed_count"] > 0 else 0.0
    )

    avg_e2e = np.mean(metrics["e2e_pipeline_latencies_ms"]) if metrics["e2e_pipeline_latencies_ms"] else 0.0
    min_e2e = np.min(metrics["e2e_pipeline_latencies_ms"]) if metrics["e2e_pipeline_latencies_ms"] else 0.0
    max_e2e = np.max(metrics["e2e_pipeline_latencies_ms"]) if metrics["e2e_pipeline_latencies_ms"] else 0.0

    avg_pure = np.mean(metrics["pure_processing_latencies_ms"]) if metrics["pure_processing_latencies_ms"] else 0.0
    min_pure = np.min(metrics["pure_processing_latencies_ms"]) if metrics["pure_processing_latencies_ms"] else 0.0
    max_pure = np.max(metrics["pure_processing_latencies_ms"]) if metrics["pure_processing_latencies_ms"] else 0.0

    print("\n\n" + "=" * 60)
    print(f"GENERATED THESIS METRICS REPORT (EXP 4 DRAIN+INT8 STRESS RUN: {metrics['processed_count']})")
    print("=" * 60)
    print(f"| Metric                          | Value                         | Statistical Scale / Context      |")
    print(f"| :---                            | :---                          | :---                             |")
    print(f"| Volume Evaluated Target         | {TOTAL_EXPECTED_ANOMALIES:<29} | Target Bounds                    |")
    print(f"| Captured Processing Count       | {metrics['processed_count']:<29} | Hard System Reliability          |")
    print(f"| Total Pipeline Processing Time  | {total_pipeline_time:<29.2f} s | Macro Wall Clock                 |")
    print(f"| Consumer Execution Throughput   | {throughput:<29.2f} logs/s | Real-Time Processing             |")
    print(f"| Mean Inference Latency (Pi 3)   | {mean_inference:<29.2f} ms | Model Core Footprint             |")
    print(f"| Mean Template Mining Latency    | {mean_template:<29.2f} ms | Drain Overhead                   |")
    print(f"| Classification IaC Accuracy     | {success_rate_iac:<29.1f} %  | Quantized IaC Accuracy Profile   |")
    print(f"| Classification Fault Accuracy   | {success_rate_fault:<29.1f} %  | Quantized Fault Accuracy Profile |")

    print(f"\n### Metrology Latency Profiles\n")
    print(f"| Latency Metric Target            | Minimum Value | Average Value | Maximum Value |")
    print(f"| :---                             | :---:         | :---:         | :---:         |")
    print(f"| **True E2E Pipeline Latency**    | {min_e2e:>8.2f} ms | {avg_e2e:>11.2f} ms | {max_e2e:>9.2f} ms |")
    print(f"| **Pure Processing Latency**      | {min_pure:>8.2f} ms | {avg_pure:>11.2f} ms | {max_pure:>9.2f} ms |")
    print("=" * 60 + "\n")

# KAFKA CONFIGURATION 
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": f"brain-exp4-drain-int8-{int(time.time())}",
    "auto.offset.reset": "latest",
})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

print(f"Quantized DRAIN+INT8 engine tracking active. Listening for exactly {TOTAL_EXPECTED_ANOMALIES} logs.")

# MAIN EXECUTION LIFECYCLE
empty_polls_count = 0
MAX_EMPTY_POLLS = 30

try:
    while metrics["processed_count"] < TOTAL_EXPECTED_ANOMALIES and not _graceful_stop:
        msg = consumer.poll(0.5)
        if msg is None:
            if metrics["processed_count"] > 0:
                empty_polls_count += 1
                if empty_polls_count >= MAX_EMPTY_POLLS:
                    print(f"\nStream idle timeout hit ({MAX_EMPTY_POLLS * 0.5}s). Pipeline shutting down.")
                    break
            continue

        empty_polls_count = 0
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        t_arrive_ns = time.time_ns()
        payload = json.loads(msg.value().decode("utf-8"))

        if metrics["start_wall_time"] is None:
            metrics["start_wall_time"] = time.time()

        raw_log = payload.get("raw_log", "")
        true_iac = payload.get("true_iac_category", "Unknown")
        true_fault = payload.get("true_fault_category", "Unknown")
        t_send_sec = payload.get("timestamp", (t_arrive_ns / 1e9))
        t_send_ns = int(t_send_sec * 1e9)
        msg_id = payload.get("msg_id", "no-id")

        # DRAIN TEMPLATE MINING
        template_start_ns = time.perf_counter_ns()
        template_text = apply_drain_template(raw_log)
        template_ms = (time.perf_counter_ns() - template_start_ns) / 1e6
        metrics["template_latencies_ms"].append(template_ms)

        # INFERENCE 
        analysis = predict_with_safety_gate(raw_log, template_text)
        metrics["inference_latencies_ms"].append(analysis["inference_ms"])

        metrics["processed_count"] += 1

        # Accuracy counters
        if analysis["iac_category"] == true_iac:
            metrics["correct_iac_classifications"] += 1
        if analysis["fault_category"] == true_fault:
            metrics["correct_fault_classifications"] += 1

        # Real-time progress line
        print(
            f"Progress [{metrics['processed_count']}/{TOTAL_EXPECTED_ANOMALIES}] | "
            f"Msg {msg_id} | Template+Infer: {template_ms + analysis['inference_ms']:.2f} ms"
        )

        # Rule distribution
        if not analysis["is_healable"]:
            if "Uncertain" in analysis["escalation_reason"]:
                metrics["rule_distribution"]["LOG_UNRECOGNIZED (Safety Gate)"] += 1
            else:
                metrics["rule_distribution"]["Manual Escalations"] += 1
        else:
            cat = analysis["iac_category"]
            if cat == "Service":
                metrics["rule_distribution"]["R3: Controlled Service Restart"] += 1
            elif cat == "Idempotency":
                metrics["rule_distribution"]["R2: Rollback to last-known-good config"] += 1
            elif cat == "Security":
                metrics["rule_distribution"]["R15: Repair File Permissions"] += 1
            elif cat == "Dependency":
                metrics["rule_distribution"]["R12: Master Node Recovery"] += 1
            else:
                metrics["rule_distribution"]["Manual Escalations"] += 1

        # Timing for queue/transport and processing
        t_publish_start_ns = time.time_ns()
        queue_to_arrival_ms = (t_arrive_ns - t_send_ns) / 1e6
        processing_ms = (t_publish_start_ns - t_arrive_ns) / 1e6

        analysis.update({
            "msg_id": msg_id,
            "t_send_ns": t_send_ns,
            "t_arrive_ns": t_arrive_ns,
            "queue_to_arrival_ms": queue_to_arrival_ms,
            "processing_ms": processing_ms,
            "true_iac_category": true_iac,
            "true_fault_category": true_fault,
        })

        target_topic = OUT_HEALABLE if analysis["is_healable"] else OUT_MANUAL
        producer.produce(target_topic, json.dumps(analysis).encode("utf-8"))
        producer.flush()

        t_publish_end_ns = time.time_ns()
        metrics["e2e_pipeline_latencies_ms"].append((t_publish_end_ns - t_send_ns) / 1e6)
        metrics["pure_processing_latencies_ms"].append((t_publish_end_ns - t_arrive_ns) / 1e6)

        # Per-log status output
        status = "HEALABLE" if analysis["is_healable"] else "MANUAL"
        print(f"\n--- {status} (DRAIN + INT8 ONNX) ---")
        print(f"Msg ID:                          {analysis['msg_id']}")
        print(f"Queue + transport time:          {analysis['queue_to_arrival_ms']:.2f} ms")
        print(f"Template + inference time:       {template_ms + analysis['inference_ms']:.2f} ms")
        print(f"Pi3 processing time:             {analysis['processing_ms']:.2f} ms")
        print(f"Predicted IaC:                   {analysis['iac_category']} (True: {analysis['true_iac_category']})")
        print(f"Predicted Fault:                 {analysis['fault_category']} (True: {analysis['true_fault_category']})")
        print("-" * 55)

    metrics["end_wall_time"] = time.time()
    if metrics["processed_count"] > 0:
        print_thesis_report()

except Exception as e:
    print(f"\nFatal error in main loop: {e}")
    metrics["end_wall_time"] = time.time()
    if metrics["processed_count"] > 0:
        print_thesis_report()

finally:
    # Persist drain state so new templates are kept across runs
    try:
        with open(state_bin, "wb") as f:
            pickle.dump(template_miner, f)
            print(f"Persisted Drain state to {state_bin}")
    except Exception as e:
        print(f"Failed to persist Drain state: {e}")

    print("Executing final producer flush and closing consumer...")
    try:
        producer.flush(timeout=5.0)
    except Exception:
        pass
    consumer.close()
    print("Inference engine stopped.")
