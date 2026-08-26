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
INTEGRATION_DIRECTORY="$(mktemp -d "${TMPDIR:-/tmp}/udpcomapp-integration.XXXXXX")"
SERVER_A_SETTINGS="${INTEGRATION_DIRECTORY}/server-a.json"
SERVER_B_SETTINGS="${INTEGRATION_DIRECTORY}/server-b.json"
SERVER_A_PACKET_LOG="${INTEGRATION_DIRECTORY}/server-a-packets.log"
SERVER_B_PACKET_LOG="${INTEGRATION_DIRECTORY}/server-b-packets.log"
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
  rm -rf "${INTEGRATION_DIRECTORY}"
}
trap cleanup EXIT INT TERM

shutdown_servers() {
  if [[ -n "${SERVER_A_PID}" ]] && kill -0 "${SERVER_A_PID}" 2>/dev/null; then
    kill -INT "${SERVER_A_PID}" 2>/dev/null || true
    wait "${SERVER_A_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SERVER_B_PID}" ]] && kill -0 "${SERVER_B_PID}" 2>/dev/null; then
    kill -INT "${SERVER_B_PID}" 2>/dev/null || true
    wait "${SERVER_B_PID}" 2>/dev/null || true
  fi
  SERVER_A_PID=""
  SERVER_B_PID=""
}

wait_for_regex() {
  local file_path="$1"
  local pattern="$2"
  for _ in {1..50}; do
    if rg -q -- "${pattern}" "${file_path}" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for ${pattern} in ${file_path}" >&2
  return 1
}

wait_for_fixed() {
  local file_path="$1"
  local pattern="$2"
  for _ in {1..50}; do
    if rg -F -q -- "${pattern}" "${file_path}" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for expected text in ${file_path}" >&2
  return 1
}

cmake -P "${PROJECT_ROOT}/scripts/download_dependencies.cmake"
cmake -S "${PROJECT_ROOT}" -B "${BUILD_DIRECTORY}" -G Ninja
cmake --build "${BUILD_DIRECTORY}"

cat >"${SERVER_A_SETTINGS}" <<EOF
{
  "listen_address": "127.0.0.1",
  "listen_port": 5000,
  "destination_address": "127.0.0.1",
  "destination_port": 5001,
  "ipc_port": 6000,
  "packet_log_path": "${SERVER_A_PACKET_LOG}"
}
EOF

cat >"${SERVER_B_SETTINGS}" <<EOF
{
  "listen_address": "127.0.0.1",
  "listen_port": 5001,
  "destination_address": "127.0.0.1",
  "destination_port": 5000,
  "ipc_port": 6001,
  "packet_log_path": "${SERVER_B_PACKET_LOG}"
}
EOF

"${BUILD_DIRECTORY}/udp_server" "${SERVER_A_SETTINGS}" \
  >"${SERVER_A_LOG}" 2>&1 &
SERVER_A_PID=$!

"${BUILD_DIRECTORY}/udp_server" "${SERVER_B_SETTINGS}" \
  >"${SERVER_B_LOG}" 2>&1 &
SERVER_B_PID=$!

# サーバが TCP IPC の待受を開始するまで少し待機する。
sleep 1

"${BUILD_DIRECTORY}/udp_sender" \
  "${SERVER_A_SETTINGS}" \
  "${PROJECT_ROOT}/examples/packets-a.txt"
"${BUILD_DIRECTORY}/udp_sender" \
  "${SERVER_B_SETTINGS}" \
  "${PROJECT_ROOT}/examples/packets-b.txt"
"${BUILD_DIRECTORY}/udp_sender" \
  "${SERVER_A_SETTINGS}" \
  "${LARGE_PAYLOAD_FILE}"

# コメント行を除いた1行が、4,000バイトのテストペイロードである。
LARGE_PAYLOAD_HEX="$(rg -v '^#' "${LARGE_PAYLOAD_FILE}" | tr -d '[:space:]')"
if [[ "${#LARGE_PAYLOAD_HEX}" -ne 8000 ]]; then
  echo "large payload fixture must contain exactly 8000 hexadecimal characters" >&2
  exit 1
fi

wait_for_regex "${SERVER_A_LOG}" 'Received\(4\): [0-9]+'
wait_for_regex "${SERVER_B_LOG}" 'Received\(4000\): [0-9]+'
shutdown_servers
wait_for_regex "${SERVER_A_PACKET_LOG}" 'Received\(4\): [0-9]+: CAFEBABE'
wait_for_regex "${SERVER_A_PACKET_LOG}" 'Received\(4\): [0-9]+: 10203040'
wait_for_regex "${SERVER_B_PACKET_LOG}" 'Received\(4\): [0-9]+: DEADBEEF'
wait_for_regex "${SERVER_B_PACKET_LOG}" 'Received\(4\): [0-9]+: 01020304'
wait_for_fixed "${SERVER_B_PACKET_LOG}" "Received(4000): "
wait_for_fixed "${SERVER_B_PACKET_LOG}" "${LARGE_PAYLOAD_HEX}"

echo "localhost integration test passed"
