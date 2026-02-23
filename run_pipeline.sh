#!/bin/bash

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
VENV_PATH="$HOME/spark-env/bin/activate"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

mkdir -p "$LOG_DIR"

# ── Helpers ───────────────────────────────────────────────────────────────────
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/pipeline_$TIMESTAMP.log"
}

run_job() {
    local job_name=$1
    local script_path="$SCRIPT_DIR/$job_name"

    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "▶ Starting: $job_name"
    log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    spark-submit "$script_path" 2>&1 | tee -a "$LOG_DIR/${job_name%.py}_$TIMESTAMP.log"
    local exit_code=${PIPESTATUS[0]}

    if [ $exit_code -ne 0 ]; then
        log "✘ FAILED: $job_name (exit code: $exit_code)"
        log "  Check log: $LOG_DIR/${job_name%.py}_$TIMESTAMP.log"
        exit $exit_code
    fi

    log "✔ Completed: $job_name"
}

# ── Activate virtual environment ──────────────────────────────────────────────
if [ -f "$VENV_PATH" ]; then
    source "$VENV_PATH"
    log "✔ Virtual environment activated"
else
    log "⚠ Virtual environment not found at $VENV_PATH — using system Python"
fi

# ── Pipeline ──────────────────────────────────────────────────────────────────
log "══════════════════════════════════════════════════"
log "  Restaurant ETL Pipeline started"
log "══════════════════════════════════════════════════"
PIPELINE_START=$(date +%s)

run_job "restaurant_etl.py"
run_job "restaurant_geocoding_etl.py"
run_job "restaurant_geohash_etl.py"
run_job "restaurant_weather_join_etl.py"

PIPELINE_END=$(date +%s)
DURATION=$((PIPELINE_END - PIPELINE_START))

log "══════════════════════════════════════════════════"
log "  ✔ All jobs completed successfully"
log "  Total duration: ${DURATION}s"
log "  Full log: $LOG_DIR/pipeline_$TIMESTAMP.log"
log "══════════════════════════════════════════════════"
