# Investment Signal Radar

VC investment data collection and foresight signal detection platform.

Automatically collects startup funding data from press releases and VC portfolio pages, extracts structured information via Claude API, and detects investment surge signals for foresight analysis.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database
python3 src/db/init_db.py

# Run collection pipeline
./scripts/run_collection.sh

# Run signal detection (Phase 3)
python3 src/analyzer/signals.py

# Export dashboard data
python3 src/analyzer/trends.py
```

## Architecture

```
RSS Feeds (PR TIMES, The Bridge)
    ↓
Collector (feedparser + keyword filter)
    ↓
Claude API (structured extraction)
    ↓
SQLite DB (investment_signal.db)
    ↓
Analyzer (trends, signals, PESTLE)
    ↓
Web Dashboard (Vite + React)
```

## Data Sources

- PR TIMES RSS (funding-related press releases)
- The Bridge RSS (startup news)
- VC Portfolio Pages (Phase 2)
- SEC EDGAR Form D (Phase 2, US data)

## Project Structure

```
src/
├── collector/    # RSS collection, web scraping
├── extractor/    # Claude API structured extraction
├── normalizer/   # Entity normalization (Phase 2)
├── analyzer/     # Trend analysis, signal detection
├── db/           # SQLite database
└── api/          # FastAPI endpoints (Phase 4)
web/              # Dashboard (Phase 4)
scripts/          # Utility scripts
data/             # Database files
```

## License

Private - Miratuku / esse-sense
