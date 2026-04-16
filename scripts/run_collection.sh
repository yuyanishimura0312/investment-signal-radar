#!/bin/bash
# Investment Signal Radar - Collection Pipeline Runner
# Usage: ./scripts/run_collection.sh
#
# Can be scheduled with cron:
#   0 * * * * cd ~/projects/research/investment-signal-radar && ./scripts/run_collection.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/collection_$(date +%Y%m%d_%H%M%S).log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

echo "[$(date)] Starting Investment Signal Radar collection pipeline..." | tee -a "$LOG_FILE"

# Initialize v2 DB if needed (idempotent)
python3 src/db/init_db_v2.py 2>&1 | tee -a "$LOG_FILE"

# Run the v2 pipeline
python3 -c "
import sys, logging
sys.path.insert(0, 'src')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('$LOG_FILE'),
        logging.StreamHandler(),
    ]
)
from collector.pipeline_v2 import run_pipeline
result = run_pipeline()
print(f'Result: {result}')
" 2>&1 | tee -a "$LOG_FILE"

# Regenerate dashboard data
python3 src/analyzer/trends_v2.py 2>&1 | tee -a "$LOG_FILE"

echo "[$(date)] Pipeline complete. Log: $LOG_FILE" | tee -a "$LOG_FILE"

# Clean up old logs (keep last 30 days)
find "$LOG_DIR" -name "collection_*.log" -mtime +30 -delete 2>/dev/null || true
