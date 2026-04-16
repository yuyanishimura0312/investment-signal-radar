#!/bin/bash
# Deploy web dashboard to GitHub Pages
# Usage: ./scripts/deploy_dashboard.sh

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "1. Generating dashboard data from DB (v2)..."
python3 -c "
import sys; sys.path.insert(0, 'src')
from analyzer.trends_v2 import export_dashboard_data
data = export_dashboard_data('web/public/data.json')
print(f'  Schema: {data[\"schema_version\"]} | {len(data[\"top_investors\"])} investors, {len(data[\"monthly_summary\"])} months')
"

echo "2. Building web dashboard..."
cd web
npx vite build
cp public/data.json dist/data.json

echo "3. Deploying to GitHub Pages..."
cd "$PROJECT_DIR"
# Use gh-pages approach: copy dist to a temp branch
DIST_DIR="web/dist"

# Create .nojekyll for GitHub Pages
touch "$DIST_DIR/.nojekyll"

# Deploy using git subtree or gh-pages
if command -v npx &> /dev/null; then
  cd web
  npx gh-pages -d dist --dotfiles 2>&1
  cd "$PROJECT_DIR"
else
  echo "Install gh-pages: npm install -g gh-pages"
  exit 1
fi

echo "4. Done! Dashboard will be available at:"
echo "   https://yuyanishimura0312.github.io/investment-signal-radar/"
