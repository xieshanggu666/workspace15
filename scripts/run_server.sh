#!/usr/bin/env bash
# 启动权威服务端(开发用, 超时参数可改)
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 -m server --host 0.0.0.0 --port 8765 --db arena.db \
  --turn-seconds 30 --response-seconds 15 --grace 15 "$@"
