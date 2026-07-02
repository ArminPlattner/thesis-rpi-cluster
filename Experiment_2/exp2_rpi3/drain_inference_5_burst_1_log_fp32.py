import os
import json
import time
import pickle
import numpy as np
import torch
import torch.nn as nn
from transformers import BertConfig, AutoTokenizer, BertModel
from confluent_kafka import Consumer, Producer
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction

# ---------------- CONFIG ----------------
MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_Model_v6_WITH_DRAIN_NEW"
DRAIN_STATE_PATH = os.path.join(MODEL_PATH, "drain_state.bin")
KAFKA_BOOTSTRAP = "10.42.0.184:32709"

IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"

CONFIDENCE_THRESHOLD = 0.90
MAX_LEN = 256
CPU_THREADS = 2

# Force PyTorch to strictly utilize only two cores to avoid thread contention spikes
torch.set_num_threads(CPU_THREADS)
torch.set_num_interop_threads(CPU_THREADS)

# ---------------- LABELS ----------------
print(f"📥 Loading labels from {MODEL_PATH}...")
with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f:
    labels_data = json.load(f)
    iac_labels = labels_data["iac_labels"]
    fault_labels = labels_data["fault_labels"]

# ---------------- CUSTOM MULTI-HEAD ARCHITECTURE ----------------
class MultiHeadTinyBERT(nn.Module):
    def __init__(self, num_iac, num_fault):
        super().__init__()
        config = BertConfig(
            hidden_size=312,
            num_hidden_layers=4,
            num_attention_heads=12,
            intermediate_size=1200,
            hidden_act="gelu",
            vocab_size=30522
        )
        # Initialize an empty skeleton config to stop BertModel from querying local directories for weights files
        self.bert = BertModel(config)
        self.dropout = nn.Dropout(0.1)
        self.iac_head = nn.Linear(312, num_iac)
        self.fault_head = nn.Linear(312, num_fault)

    def forward(self, input_ids, attention_mask):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs.last_hidden_state[:, 0]  # Extract CLS token marker
        pooled_output = self.dropout(pooled_output)
        return self.iac_head(pooled_output), self.fault_head(pooled_output)

# ---------------- DRAIN PARSER SETUP ----------------
config = TemplateMinerConfig()
config.masking_instructions = [
    MaskingInstruction(r"\*+", "STARS"),
    MaskingInstruction(r"(?<=TASK \[).+?(?=\])", "TASK_NAME"),
    MaskingInstruction(r"\b(?:[A-Za-z0-9_-]+\.)+[A-Za-z]{2,}\b", "HOST"),
]
config.drain_depth = 6
config.drain_sim_th = 0.7

if os.path.exists(DRAIN_STATE_PATH):
    print(f"🔄 Restoring Drain tree state structure from {DRAIN_STATE_PATH}...")
    with open(DRAIN_STATE_PATH, "rb") as f:
        template_miner = pickle.load(f)
else:
    print("⚠️ State tree binary missing. Instantiating vanilla TemplateMiner cluster configuration...")
    template_miner = TemplateMiner(config=config)

# ---------------- LOAD CUSTOM PYTORCH PIPELINE ----------------
print(f"🔥 Loading un-quantized FP32 Multi-Head PyTorch unified model weights...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)

device = torch.device("cpu")
model = MultiHeadTinyBERT(len(iac_labels), len(fault_labels))
weights_file = os.path.join(MODEL_PATH, "model_weights.bin")

if os.path.exists(weights_file):
    # Map the combined layout file cleanly onto our structural config
    state = torch.load(weights_file, map_location=device)
    model.load_state_dict(state, strict=True)
else:
    raise FileNotFoundError(f"❌ Critical Error: model_weights.bin weights not found at {weights_file}")

model.to(device).eval()  # Hard set inference mode
print("✅ Multi-Head PyTorch Model successfully mapped and structured on Pi3 CPU!")

# ---------------- HELPERS ----------------
def clean_log(log_line):
    lines = str(log_line).splitlines()
    filtered = [l.strip() for l in lines if l.strip() and not l.strip().startswith("PLAY")]
    return " ".join(filtered).replace('""', '"')

def apply_drain_template(raw_log):
    flattened_log = clean_log(raw_log)
    match = template_miner.match(flattened_log)
    if match is not None:
        return match.get_template()
    result = template_miner.add_log_message(flattened_log)
    return result["template_mined"]

def get_sub_system(text):
    t = text.lower()
    if any(k in t for k in ["mysql", "mariadb", "postgres", "sqlite", "database"]):
        return "Storage System"
    if any(k in t for k in ["port", "tcp", "bind", "address", "dhcp", "ip", "ssh"]):
        return "Network Setup"
    if any(k in t for k in ["permission", "chmod", "chown", "directory", "path"]):
        return "File System"
    return "General Infrastructure"

def predict_with_safety_gate(raw_log, model_input):
    infer_start_ns = time.perf_counter_ns()

    inputs = tokenizer(
        model_input,
        truncation=True,
        padding="max_length",
        max_length=MAX_LEN,
        return_tensors="pt"
    )
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        logits_iac, logits_fault = model(
            input_ids=inputs["input_ids"],
            attention_mask=inputs["attention_mask"]
        )

        probs_iac = torch.softmax(logits_iac[0], dim=-1).cpu().numpy()
        probs_fault = torch.softmax(logits_fault[0], dim=-1).cpu().numpy()

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
        "template_text": model_input,
        "iac_category": iac_cat,
        "iac_confidence": iac_conf,
        "fault_category": fault_cat,
        "fault_confidence": fault_conf,
        "sub_system": get_sub_system(model_input),
        "is_healable": is_healable,
        "escalation_reason": reason,
        "inference_ms": inference_ms
    }

# ---------------- KAFKA DATA PIPELINE LOOP ----------------
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "brain-drain-fp32-v5",
    "auto.offset.reset": "latest",
})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

print(f"📡 DRAIN PyTorch FP32 Baseline listening active on {IN_TOPIC}...")

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

        template_text = apply_drain_template(raw_log)
        analysis = predict_with_safety_gate(raw_log, template_text)

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
        print(f"\n--- {status} (DRAIN + NATIVE PYTORCH FP32) ---")
        print(f"Msg ID:                    {msg_id}")
        print(f"Queue + transport time:    {queue_to_arrival_ms:.2f} ms")
        print(f"Template + inference time: {analysis['inference_ms']:.2f} ms")
        print(f"Pi3 processing time:       {processing_ms:.2f} ms")
        print(f"Full pipeline execution:   {full_pipeline_ms:.2f} ms")
        print(f"Predicted IaC:             {analysis['iac_category']} (True: {true_iac})")
        print(f"Predicted Fault:           {analysis['fault_category']} (True: {true_fault})")
        print("-" * 55)

except KeyboardInterrupt:
    print("\n🛑 Shutting down PyTorch baseline consumer...")
finally:
    consumer.close()
