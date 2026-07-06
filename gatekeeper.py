import json
import time
import sys
from confluent_kafka import Consumer, Producer

# CONFIGURATION CONSTANTS
KAFKA_BOOTSTRAP = "10.42.0.184:32709"
IN_TOPIC = "ansible-raw-logs"
OUT_TOPIC = "ansible-anomalous"

# KAFKA INITIALIZATION
c = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": f"gatekeeper-core-production-{int(time.time())}",
    "auto.offset.reset": "latest"
})

p = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 0,
    "acks": 1
})

c.subscribe([IN_TOPIC])
print("Gatekeeper Core Active: Stream filtering engine operational.")

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka Consumer Error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode("utf-8"))
            raw_log = payload.get("raw_log", "")
            raw_log_lower = raw_log.lower().strip()

            # Based on the render_fatal function in the log generation:
            # 1. 'fatal:' covers failed_task and unreachable.
            # 2. 'failed:' covers failed_inline.
            # 3. 'error!' covers all error_global_* variants.
            is_anomaly = (
                "fatal:" in raw_log_lower or 
                "failed:" in raw_log_lower or 
                "error!" in raw_log_lower
            )
.
            is_nominal = (
                "ok:" in raw_log_lower or 
                "changed:" in raw_log_lower
            )

            # ROUTING DETERMINATION
            if is_anomaly:
                # We forward if it matches an anomaly signature, regardless of status lines
                p.produce(OUT_TOPIC, json.dumps(payload).encode("utf-8"))
                p.poll(0)
                print(f"Anomaly Intercepted & Forwarded: {payload.get('msg_id', 'no-id')}")
            elif is_nominal:
                # Discard healthy operations
                print(f"Normal Log Dropped: {payload.get('msg_id', 'no-id')}")
            else:
                # Catch-all for logs that don't match either (e.g., malformed or noise)
                print(f"Unknown Log Pattern Dropped: {payload.get('msg_id', 'no-id')}")

        except json.JSONDecodeError:
            print("Parsing Error: Invalid JSON syntax inside log frame.")
        except Exception as loop_ex:
            print(f"Worker Loop Anomaly: {loop_ex}")

except KeyboardInterrupt:
    print("\nIntercepting termination command... Draining gatekeeper line buffers...")
finally:
    print("Executing final wire synchronization flush...")
    p.flush(timeout=5.0)
    c.close()
    print("Gatekeeper offline.")
