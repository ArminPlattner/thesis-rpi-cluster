#!/bin/bash

# --- CONFIG ---
VENV_PATH="/home/ubuntu/ansible-intelligence/venv"
METRICS_DIR="/home/ubuntu/ansible-intelligence/perf_metrics"
SCRIPT_NAME="drainless_inference_5_burst_10_logs.py"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
NMON_OUT="$METRICS_DIR/v6_exp3_drainless_int8_profile_$TIMESTAMP.nmon"

# Total duration: 5s (safety warmup) + 60s (log ingestion window) + 10s (cooldown) = 75s
TOTAL_DURATION=100

mkdir -p "$METRICS_DIR"

echo "==========================================================="
echo "   Pi3 Phase 6: Experiment 3 Automation Execution Runner   "
echo "==========================================================="
echo "📊 Instantiating background profiling layer for ${TOTAL_DURATION}s..."

# 1. Start nmon
nmon -f -s 1 -c $TOTAL_DURATION -F "$NMON_OUT"
NMON_PID=$(pgrep -n nmon)
echo "✅ nmon active [PID: $NMON_PID]. Target tracking file:"
echo "   $NMON_OUT"

# 2. Pre-execution baseline (5 seconds)
echo "⏳ Capturing 5s baseline..."
sleep 5

echo "🚀 Activating virtual environment and spawning engine..."
source "$VENV_PATH/bin/activate"
python3 "$SCRIPT_NAME"

# 3. Post-execution cool down (10 seconds)
echo "⏳ Capturing 10s cool down..."
sleep 10

echo "-----------------------------------------------------------"
echo "🛑 Stopping active loops and flushing buffers..."

# Safely terminate background nmon job
if [ -n "$NMON_PID" ]; then
    kill "$NMON_PID" 
    sleep 2
    echo "🏁 Environment flushed. Metrics file locked cleanly."
else
    echo "⚠️ Warning: nmon process was not detected active."
fi

deactivate
