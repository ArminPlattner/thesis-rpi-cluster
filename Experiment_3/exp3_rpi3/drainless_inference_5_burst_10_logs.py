import os
import json
import time
import pickle
import numpy as np
from transformers import AutoTokenizer
from confluent_kafka import Consumer, Producer
import onnxruntime as ort
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.masking import MaskingInstruction

# CONFIGURATION
MODEL_PATH = "/home/ubuntu/ansible-intelligence/RPI_Cluster_Models/BSc_Thesis_V6_Quantized_DRAIN"
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
TOTAL_EXPECTED_ANOMALIES = 50
IN_TOPIC = "ansible-anomalous"
OUT_HEALABLE = "ansible-classified"
OUT_MANUAL = "ansible-manual-intervention"

# METRICS
metrics = {
    "inference_latencies_ms": []
}

# INITIALIZATION 
with open(os.path.join(MODEL_PATH, "labels.json"), "r") as f:
    labels = json.load(f)
    iac_labels, fault_labels = labels["iac_labels"], labels["fault_labels"]

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
onnx_files = [f for f in os.listdir(MODEL_PATH) if f.endswith(".onnx")]
session = ort.InferenceSession(os.path.join(MODEL_PATH, onnx_files[0]), providers=["CPUExecutionProvider"])

# DRAIN3 Setup
config = TemplateMinerConfig()
config.masking_instructions = [MaskingInstruction(r"\*+", "STARS"), MaskingInstruction(r"(?<=TASK \[).+?(?=\])", "TASK_NAME")]
config.drain_depth = 6
config.drain_sim_th = 0.7
state_bin = os.path.join(MODEL_PATH, "drain_state.bin")
template_miner = pickle.load(open(state_bin, "rb")) if os.path.exists(state_bin) else TemplateMiner(config=config)

def apply_drain(raw_log):
    match = template_miner.match(str(raw_log).strip())
    return match.get_template() if match else template_miner.add_log_message(str(raw_log).strip())["template_mined"]

def softmax(x):
    e = np.exp(x - np.max(x, axis=-1, keepdims=True))
    return e / e.sum(axis=-1, keepdims=True)

def predict_drain_pipeline(raw_log, template):
    start_ns = time.perf_counter_ns()
    inputs = tokenizer(template, truncation=True, padding="max_length", max_length=256, return_tensors="np")
    onnx_inputs = {"input_ids": inputs["input_ids"].astype(np.int64), "attention_mask": inputs["attention_mask"].astype(np.int64)}
    outputs = session.run(None, onnx_inputs)
    
    iac_cat, fault_cat = iac_labels[int(np.argmax(softmax(outputs[0][0])))], fault_labels[int(np.argmax(softmax(outputs[1][0])))]
    inference_ms = (time.perf_counter_ns() - start_ns) / 1e6
    
    is_healable = not (iac_cat in ["Syntax", "Documentation"] or fault_cat in ["Typos", "Variable Misreference", "Incorrect Task Iteration Logic"])
    
    return {"iac_category": iac_cat, "fault_category": fault_cat, "is_healable": is_healable, "inference_ms": inference_ms}

# MAIN LOOP
consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"exp3-drain-{int(time.time())}", "auto.offset.reset": "latest"})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
consumer.subscribe([IN_TOPIC])

print("DRAIN + INT8 Pipeline Active")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error(): continue
        
        t_arr = time.time_ns()
        payload = json.loads(msg.value().decode("utf-8"))

        template = apply_drain(payload["raw_log"])
        analysis = predict_drain_pipeline(payload["raw_log"], template)
        
        metrics["inference_latencies_ms"].append(analysis["inference_ms"])
        
        t_pub = time.time_ns()
        analysis.update({
            "msg_id": payload.get("msg_id"),
            "queue_to_arrival_ms": (t_arr - payload.get("t_send_ns", t_arr)) / 1e6,
            "processing_ms": (t_pub - t_arr) / 1e6
        })
        
        producer.produce(OUT_HEALABLE if analysis["is_healable"] else OUT_MANUAL, json.dumps(analysis).encode("utf-8"))
        producer.flush()

        # Feedback Output
        status = "HEALABLE" if analysis["is_healable"] else "MANUAL"
        print(f"\n--- {status} (DRAIN + INT8 ONNX) ---")
        print(f"Msg ID:                    {analysis['msg_id']}")
        print(f"Queue + transport time:    {analysis['queue_to_arrival_ms']:.2f} ms")
        print(f"Template + inference time: {analysis['inference_ms']:.2f} ms")
        print(f"Pi3 processing time:       {analysis['processing_ms']:.2f} ms")
        print(f"Predicted IaC:             {analysis['iac_category']} (True: {payload.get('true_iac_category', 'N/A')})")
        print(f"Predicted Fault:           {analysis['fault_category']} (True: {payload.get('true_fault_category', 'N/A')})")
        print("-" * 55)

        if len(metrics["inference_latencies_ms"]) >= TOTAL_EXPECTED_ANOMALIES:
            print("Target reached. Closing pipeline.")
            break
except KeyboardInterrupt:
    print("Shutting down.")
finally:
    consumer.close()
