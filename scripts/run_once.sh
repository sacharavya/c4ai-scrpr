#!/usr/bin/env bash
set -euo pipefail

python -m app.main crawl --type events --limit 25 "$@"
