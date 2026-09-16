#!/usr/bin/env bash
# 全量测试(真实 WebSocket 链路: 超时/重连/重启)
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 -m pytest tests/ -v "$@"
