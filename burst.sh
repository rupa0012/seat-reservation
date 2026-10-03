#!/usr/bin/env sh
# Usage: ./burst.sh <BASE_URL>      e.g. ./burst.sh https://your-app.onrender.com
set -e
BASE_URL="${1:-http://localhost:8000}"
python -m pip install --quiet httpx
python scripts/burst.py "$BASE_URL"
