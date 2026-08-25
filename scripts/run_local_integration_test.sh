#!/usr/bin/env bash
# macOS / Ubuntu Linux 上で localhost の双方向 UDP 通信を検証する。
# 実行方法: bash scripts/run_local_integration_test.sh

set -euo pipefail

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIRECTORY}/.." && pwd)"
BUILD_DIRECTORY="${PROJECT_ROOT}/build"
SERVER_A_LOG="${BUILD_DIRECTORY}/server-a-integration.log"
SERVER_B_LOG="${BUILD_DIRECTORY}/server-b-integration.log"
LARGE_PAYLOAD_FILE="${PROJECT_ROOT}/examples/packets-large.txt"
SERVER_A_PID=""
SERVER_B_PID=""

cleanup() {
  if [[ -n "${SERVER_A_PID}" ]] && kill -0 "${SERVER_A_PID}" 2>/dev/null; then
    kill "${SERVER_A_PID}" 2>/dev/null || true
    wait "${SERVER_A_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SERVER_B_PID}" ]] && kill -0 "${SERVER_B_PID}" 2>/dev/null; then
    kill "${SERVER_B_PID}" 2>/dev/null || true
    wait "${SERVER_B_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

cmake -P "${PROJECT_ROOT}/scripts/download_dependencies.cmake"
cmake -S "${PROJECT_ROOT}" -B "${BUILD_DIRECTORY}" -G Ninja
cmake --build "${BUILD_DIRECTORY}"

"${BUILD_DIRECTORY}/udp_server" "${PROJECT_ROOT}/examples/server-a.json" \
  >"${SERVER_A_LOG}" 2>&1 &
SERVER_A_PID=$!

"${BUILD_DIRECTORY}/udp_server" "${PROJECT_ROOT}/examples/server-b.json" \
  >"${SERVER_B_LOG}" 2>&1 &
SERVER_B_PID=$!

# サーバが TCP IPC の待受を開始するまで少し待機する。
sleep 1

"${BUILD_DIRECTORY}/udp_sender" \
  "${PROJECT_ROOT}/examples/server-a.json" \
  "${PROJECT_ROOT}/examples/packets-a.txt"
"${BUILD_DIRECTORY}/udp_sender" \
  "${PROJECT_ROOT}/examples/server-b.json" \
  "${PROJECT_ROOT}/examples/packets-b.txt"
"${BUILD_DIRECTORY}/udp_sender" \
  "${PROJECT_ROOT}/examples/server-a.json" \
  "${LARGE_PAYLOAD_FILE}"

# コメント行を除いた1行が、4,000バイトのテストペイロードである。
LARGE_PAYLOAD_HEX="$(rg -v '^#' "${LARGE_PAYLOAD_FILE}" | tr -d '[:space:]')"
if [[ "${#LARGE_PAYLOAD_HEX}" -ne 8000 ]]; then
  echo "large payload fixture must contain exactly 8000 hexadecimal characters" >&2
  exit 1
fi

rg -q 'Received\(4\): CAFEBABE' "${SERVER_A_LOG}"
rg -q 'Received\(4\): 10203040' "${SERVER_A_LOG}"
rg -q 'Received\(4\): DEADBEEF' "${SERVER_B_LOG}"
rg -q 'Received\(4\): 01020304' "${SERVER_B_LOG}"
rg -F -q "Received(4000): ${LARGE_PAYLOAD_HEX}" "${SERVER_B_LOG}"

echo "localhost integration test passed"
