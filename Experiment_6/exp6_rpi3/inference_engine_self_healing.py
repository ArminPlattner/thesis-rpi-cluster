#!/usr/bin/env python3
import json, os, pickle, time, numpy as np, onnxruntime as ort, signal, argparse
from confluent_kafka import Consumer, Producer
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction
from transformers import AutoTokenizer

MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_V6_Quantized_DRAIN"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"
CONFIDENCE_THRESHOLD = 0.90
MAX_LEN = 256

# Metrics tracking
metrics = {
    "start_wall_time": None,
    "processed_count": 0,
    "correct_iac": 0,
    "inference_ms": [],
    "template_ms": []
}

def softmax(x):
    x = x - np.max(x, axis=-1, keepdims=True)
    e = np.exp(x)
    return e / np.sum(e, axis=-1, keepdims=True)

def load_template_miner(path):
    config = TemplateMinerConfig()
    config.masking_instructions = [
        MaskingInstruction(r"\*+", "STARS"),
        MaskingInstruction(r"(?<=TASK \[).+?(?=\])", "TASK_NAME"),
        MaskingInstruction(r"\b(?:[A-Za-z0-9_-]+\.)+[A-Za-z]{2,}\b", "HOST"),
    ]
    config.drain_depth = 6
    config.drain_sim_th = 0.7
    state_bin = os.path.join(path, "drain_state.bin")
    if os.path.exists(state_bin):
        with open(state_bin, "rb") as f: return pickle.load(f), state_bin
    return TemplateMiner(config=config), state_bin

def main():
    # Load labels
    with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f: data = json.load(f)
    iac_labels, fault_labels = data["iac_labels"], data["fault_labels"]
    
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    
    # Dynamic model loading
    onnx_files = [f for f in os.listdir(MODEL_PATH) if f.endswith(".onnx")]
    if not onnx_files: raise FileNotFoundError(f"No ONNX model in {MODEL_PATH}")
    session = ort.InferenceSession(os.path.join(MODEL_PATH, onnx_files[0]))
    
    template_miner, state_bin = load_template_miner(MODEL_PATH)
    
    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"pi3-inference-{int(time.time())}"})
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([IN_TOPIC])

    print(f"🚀 Inference engine active using {onnx_files[0]}")
    
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None or msg.error(): continue
            
            payload = json.loads(msg.value().decode("utf-8"))
            if metrics["start_wall_time"] is None: metrics["start_wall_time"] = time.time()
            
            # Drain
            t0 = time.perf_counter_ns()
            template_text = template_miner.add_log_message(str(payload["raw_log"]).strip())["template_mined"]
            template_ms = (time.perf_counter_ns() - t0) / 1e6
            
            # Inference
            t1 = time.perf_counter_ns()
            inputs = tokenizer(template_text, truncation=True, padding="max_length", max_length=MAX_LEN, return_tensors="np")
            outputs = session.run(None, {"input_ids": inputs["input_ids"].astype(np.int64), "attention_mask": inputs["attention_mask"].astype(np.int64)})
            inference_ms = (time.perf_counter_ns() - t1) / 1e6
            
            probs_iac = softmax(outputs[0][0]); iac_idx = int(np.argmax(probs_iac))
            probs_fault = softmax(outputs[1][0]); fault_idx = int(np.argmax(probs_fault))
            
            # Result
            is_healable = iac_labels[iac_idx] not in {"Syntax", "Documentation"} and float(probs_iac[iac_idx]) >= CONFIDENCE_THRESHOLD
            
            metrics["processed_count"] += 1
            if iac_labels[iac_idx] == payload.get("true_iac"): metrics["correct_iac"] += 1
            
            result = {**payload, "iac_category": iac_labels[iac_idx], "fault_category": fault_labels[fault_idx], "is_healable": is_healable}
            
            producer.produce(OUT_HEALABLE if is_healable else OUT_MANUAL, json.dumps(result).encode("utf-8"))
            producer.flush()
            
            print(f"[{metrics['processed_count']}] IaC: {result['iac_category']} | Fault: {result['fault_category']} | Healable: {is_healable}")
            
    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()
