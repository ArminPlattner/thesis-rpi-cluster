import os
import json
import time
import pickle
import numpy as np
from transformers import AutoTokenizer
from confluent_kafka import Consumer, Producer
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction
import onnxruntime as ort

# CONFIG
MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_V6_Quantized_DRAIN"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"

IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"

CONFIDENCE_THRESHOLD = 0.90
MAX_LEN = 256
CPU_THREADS = 2

# LABELS
with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f:
    labels_data = json.load(f)
    iac_labels = labels_data["iac_labels"]
    fault_labels = labels_data["fault_labels"]

# DRAIN PARSER SETUP 
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
    with open(state_bin, "rb") as f:
        template_miner = pickle.load(f)
else:
    template_miner = TemplateMiner(config=config)

# ONNX ENGINE SETUP
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
onnx_files = [f for f in os.listdir(MODEL_PATH) if f.endswith(".onnx")]
ONNX_PATH = os.path.join(MODEL_PATH, onnx_files[0])
sess_options = ort.SessionOptions()
sess_options.intra_op_num_threads = CPU_THREADS
session = ort.InferenceSession(ONNX_PATH, sess_options, providers=["CPUExecutionProvider"])

def softmax(x):
    x = x - np.max(x, axis=-1, keepdims=True)
    ex = np.exp(x)
    return ex / ex.sum(axis=-1, keepdims=True)

def get_sub_system(text):
    t = text.lower()
    if any(k in t for k in ["mysql", "mariadb", "postgres", "sqlite", "database"]):
        return "Storage System"
    if any(k in t for k in ["port", "tcp", "bind", "address", "dhcp", "ip", "ssh"]):
        return "Network Setup"
    if any(k in t for k in ["permission", "chmod", "chown", "directory", "path"]):
        return "File System"
    return "General Infrastructure"

def apply_drain_template(raw_log):
    flattened = str(raw_log).strip()
    match = template_miner.match(flattened)
    if match: return match.get_template()
    return template_miner.add_log_message(flattened)["template_mined"]

def predict_with_safety_gate(raw_log, model_input):
    infer_start_ns = time.perf_counter_ns()
    
    inputs = tokenizer(model_input, truncation=True, padding="max_length", 
                       max_length=MAX_LEN, return_tensors="np")
    
    onnx_inputs = {
        "input_ids": inputs["input_ids"].astype(np.int64),
        "attention_mask": inputs["attention_mask"].astype(np.int64),
    }
    
    outputs = session.run(None, onnx_inputs)
    probs_iac = softmax(outputs[0][0])
    probs_fault = softmax(outputs[1][0])
    
    infer_ms = (time.perf_counter_ns() - infer_start_ns) / 1e6
    iac_idx = int(np.argmax(probs_iac))
    fault_idx = int(np.argmax(probs_fault))
    
    is_healable = True
    manual_faults = ["Typos"]
    reason = "High confidence autonomic match"

    if iac_labels[iac_idx] in ["Syntax", "Documentation"] or fault_labels[fault_idx] in manual_faults:
        is_healable = False
        reason = f"Manual Intervention: {fault_labels[fault_idx]}"
    elif float(probs_iac[iac_idx]) < CONFIDENCE_THRESHOLD:
        is_healable = False
        reason = "Uncertain Classification"

    return {
        "raw_log": raw_log, "template_text": model_input,
        "iac_category": iac_labels[iac_idx], "iac_confidence": float(probs_iac[iac_idx]),
        "fault_category": fault_labels[fault_idx], "fault_confidence": float(probs_fault[fault_idx]),
        "sub_system": get_sub_system(model_input),
        "is_healable": is_healable, "escalation_reason": reason, "inference_ms": infer_ms
    }

# PIPELINE 
consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "drain-int8-v6", "auto.offset.reset": "latest"})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

print(f"DRAIN INT8 ONNX listening active on {IN_TOPIC}...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): continue

        t_arrive_ns = time.time_ns()
        payload = json.loads(msg.value().decode("utf-8"))
        
        template = apply_drain_template(payload["raw_log"])
        analysis = predict_with_safety_gate(payload["raw_log"], template)
        t_after_infer_ns = time.time_ns()

        analysis.update({
            "msg_id": payload["msg_id"],
            "true_iac_category": payload.get("true_iac_category"),
            "true_fault_category": payload.get("true_fault_category"),
            "queue_to_arrival_ms": (t_arrive_ns - payload["t_send_ns"]) / 1e6,
            "processing_ms": (t_after_infer_ns - t_arrive_ns) / 1e6
        })

        target_topic = OUT_HEALABLE if analysis["is_healable"] else OUT_MANUAL
        producer.produce(target_topic, json.dumps(analysis).encode("utf-8"))
        producer.flush()
        
        status = "HEALABLE" if analysis["is_healable"] else "MANUAL"
        print(f"\n--- {status} (DRAIN + INT8 ONNX) ---")
        print(f"Msg ID:                    {analysis['msg_id']}")
        print(f"Queue + transport time:    {analysis['queue_to_arrival_ms']:.2f} ms")
        print(f"Template + inference time: {analysis['inference_ms']:.2f} ms")
        print(f"Pi3 processing time:       {analysis['processing_ms']:.2f} ms")
        print(f"Predicted IaC:             {analysis['iac_category']} (True: {analysis['true_iac_category']})")
        print(f"Predicted Fault:           {analysis['fault_category']} (True: {analysis['true_fault_category']})")
        print("-" * 55)
except KeyboardInterrupt:
    consumer.close()
