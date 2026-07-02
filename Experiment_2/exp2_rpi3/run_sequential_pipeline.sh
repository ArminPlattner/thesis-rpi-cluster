#!/bin/bash

METRICS_DIR="/home/ubuntu/ansible-intelligence/perf_metrics"
mkdir -p "$METRICS_DIR"

echo "==========================================================="
echo "   Pi3 Phase 6 Synchronized Profiler (5 Bursts, 1 Log)"
echo "==========================================================="
echo "Select the execution target to profile:"
echo "1) DRAIN + Full PyTorch FP32 Baseline     (drain_inference_5_burst_1_log_fp32.py)"
echo "2) DRAIN + Quantized INT8 ONNX            (drain_inference_5_burst_1_log_quantized.py)"
echo "3) DRAINless Quantized INT8 ONNX          (drainless_inference_5_burst_1_log.py)"
echo "4) DRAINless Full PyTorch FP32 Baseline   (drainless_inference_5_burst_1_logs_fp32.py)"
echo "==========================================================="
read -p "Selection [1-4]: " choice

case $choice in
    1) SCRIPT="drain_inference_5_burst_1_log_fp32.py"; PREFIX="v6_drain_fp32" ;;
    2) SCRIPT="drain_inference_5_burst_1_log_quantized.py"; PREFIX="v6_drain_int8" ;;
    3) SCRIPT="drainless_inference_5_burst_1_log.py"; PREFIX="v6_drainless_int8" ;;
    4) SCRIPT="drainless_inference_5_burst_1_logs_fp32.py"; PREFIX="v6_drainless_fp32" ;;
    *) echo "Invalid choice. Exiting."; exit 1 ;;
esac

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
NMON_OUT="$METRICS_DIR/${PREFIX}_profile_${TIMESTAMP}.nmon"

# Activation of environment before scheduling
source /home/ubuntu/ansible-intelligence/venv/bin/activate

# Synchronization Trigger: Start both exactly 5 seconds from now
SYNC_TIME=$(date -d "+5 seconds" +%s)
echo "🚀 Target $SCRIPT selected."
echo "⏳ Synchronization delay: Starting in 5 seconds at $SYNC_TIME..."

# Wait loop
while [ $(date +%s) -lt $SYNC_TIME ]; do sleep 0.5; done

echo "Spawning synchronized capture..."
# Start nmon and Python simultaneously
nmon -f -s 1 -c 300 -F "$NMON_OUT" &
python3 /home/ubuntu/ansible-intelligence/exp_2/$SCRIPT &
PYTHON_PID=$!

echo "Monitoring active. Target file: $NMON_OUT"

cleanup() {
    echo -e "\nStopping processes..."
    kill $PYTHON_PID 2>/dev/null
    echo "Capture complete."
    exit 0
}

trap cleanup SIGINT

wait $PYTHON_PID
cleanup
