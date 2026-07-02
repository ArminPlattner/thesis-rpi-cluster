import os
import json
import time
import numpy as np
from transformers import AutoTokenizer
from confluent_kafka import Consumer, Producer
import onnxruntime as ort

# ---------------- CONFIG ----------------
MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_V6_Quantized_Drainless"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"

IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"

CONFIDENCE_THRESHOLD = 0.90
MAX_LEN = 256
CPU_THREADS = 2

# ---------------- LABELS ----------------
print(f"📥 Loading labels from {MODEL_PATH}...")
with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f:
    labels_data = json.load(f)
    iac_labels = labels_data["iac_labels"]
    fault_labels = labels_data["fault_labels"]

# ---------------- TOKENIZER + ONNX ----------------
print(f"🔄 Initializing Tokenizer from {MODEL_PATH}...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)

onnx_files = [f for f in os.listdir(MODEL_PATH) if f.endswith(".onnx")]
if not onnx_files:
    raise FileNotFoundError(f"❌ Critical Error: No ONNX runtime model found in {MODEL_PATH}")
ONNX_PATH = os.path.join(MODEL_PATH, onnx_files[0])

sess_options = ort.SessionOptions()
sess_options.intra_op_num_threads = CPU_THREADS
sess_options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL

session = ort.InferenceSession(ONNX_PATH, sess_options, providers=["CPUExecutionProvider"])
print(f"✅ Quantized INT8 DRAINless Engine loaded: {ONNX_PATH}")

def softmax(x):
    x = x - np.max(x, axis=-1, keepdims=True)
    ex = np.exp(x)
    return ex / ex.sum(axis=-1, keepdims=True)

def predict_with_safety_gate(raw_log):
    infer_start_ns = time.perf_counter_ns()

    # Step A: Feed the RAW log directly to tokenizer (DRAIN bypass)
    inputs = tokenizer(
        raw_log,
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
    logits_iac, logits_fault = outputs[0], outputs[1]

    probs_iac = softmax(logits_iac[0])
    probs_fault = softmax(logits_fault[0])

    infer_end_ns = time.perf_counter_ns()
    inference_ms = (infer_end_ns - infer_start_ns) / 1e6

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
        "template_text": "BYPASS_DRAINLESS",
        "iac_category": iac_cat,
        "iac_confidence": iac_conf,
        "fault_category": fault_cat,
        "fault_confidence": fault_conf,
        "sub_system": "General Infrastructure", # Fallback baseline
        "is_healable": is_healable,
        "escalation_reason": reason,
        "inference_ms": inference_ms
    }

# ---------------- KAFKA DATA PIPELINE LOOP ----------------
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "brain-drainless-int8-v6",
    "auto.offset.reset": "latest",
})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

print(f"🧠 DRAINless ONNX engine streaming from {IN_TOPIC}...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error(): continue

        t_arrive_ns = time.time_ns()
        payload = json.loads(msg.value().decode("utf-8"))

        raw_log = payload["raw_log"]
        t_send_ns = payload["t_send_ns"]
        msg_id = payload.get("msg_id", "no-id")
        true_iac = payload.get("true_iac_category", None)
        true_fault = payload.get("true_fault_category", None)

        analysis = predict_with_safety_gate(raw_log)

        t_after_infer_ns = time.time_ns()

        queue_to_arrival_ms = (t_arrive_ns - t_send_ns) / 1e6
        processing_ms = (t_after_infer_ns - t_arrive_ns) / 1e6

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
        analysis["target_topic"] = target_topic

        producer.produce(target_topic, json.dumps(analysis).encode("utf-8"))
        producer.flush()

        t_publish_end_ns = time.time_ns()
        full_pipeline_ms = (t_publish_end_ns - t_send_ns) / 1e6

        status = "HEALABLE" if analysis["is_healable"] else "MANUAL"
        print(f"\n--- {status} (DRAINLESS INT8 ONNX) ---")
        print(f"Msg ID:                    {msg_id}")
        print(f"Queue + transport time:    {queue_to_arrival_ms:.2f} ms")
        print(f"Pure inference time:       {analysis['inference_ms']:.2f} ms")
        print(f"Pi3 processing time:       {processing_ms:.2f} ms")
        print(f"Full pipeline execution:   {full_pipeline_ms:.2f} ms")
        print(f"Predicted IaC:             {analysis['iac_category']} (True: {true_iac})")
        print(f"Predicted Fault:           {analysis['fault_category']} (True: {true_fault})")
        print("-" * 55)

except KeyboardInterrupt:
    print("\n🛑 Shutting down drainless consumer...")
finally:
    consumer.close()
