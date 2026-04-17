#!/bin/bash
# Enrich organization data via gBizINFO government API.
# Looks up Japanese companies missing corporate_number and fills in
# capital, employee count, and address from official records.
#
# Usage:
#   ./scripts/enrich_organizations.sh          # enrich up to 50 orgs
#   ./scripts/enrich_organizations.sh --dry-run # preview without DB updates
#   ./scripts/enrich_organizations.sh --limit 100

set -euo pipefail
cd "$(dirname "$0")/.."

LIMIT=50
DRY_RUN="False"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN="True"; shift ;;
        --limit) LIMIT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "Enriching organizations via gBizINFO (limit=$LIMIT, dry_run=$DRY_RUN)..."

python3 -c "
import sys, logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
sys.path.insert(0, '.')
from src.integrations.enrichment_pipeline import enrich_organizations, get_enrichment_stats

result = enrich_organizations('data/investment_signal_v2.db', limit=$LIMIT, dry_run=$DRY_RUN)
print()
print('=== Enrichment Result ===')
for k, v in result.items():
    print(f'  {k}: {v}')

print()
stats = get_enrichment_stats('data/investment_signal_v2.db')
print('=== Current Stats ===')
for k, v in stats.items():
    print(f'  {k}: {v}')
"
