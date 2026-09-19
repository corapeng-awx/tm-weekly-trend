#!/bin/bash
# Refresh dashboard data locally and push to GitHub Pages.
# Usage: ./refresh-and-push.sh
set -e
cd "$(dirname "$0")"

echo "=== Refreshing TM Dashboard ==="
python3 refresh.py

if git diff --quiet index.html; then
    echo "No data changes — skipping push."
else
    git add index.html
    git commit -m "data: refresh $(date +%Y-%m-%d)"
    git push
    echo "Pushed. Dashboard will update at https://corapeng-awx.github.io/tm-weekly-trend/"
fi
