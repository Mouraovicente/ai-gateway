#!/usr/bin/env bash
# Local benchmark: gateway overhead in isolation from the LLM backend.
# Starts a stub Ollama backend + the gateway on :18090, runs the hey
# scenarios from docs/benchmarks/2026-09-20-local.md, and prints a one-line
# summary per run.
#
# Usage:
#   bash bench/run.sh            # STORE_BACKEND=dynamodb (LocalStack), end-to-end
#   bash bench/run.sh memory     # STORE_BACKEND=memory, gateway overhead only
#
# In memory mode nothing outside the process is touched: tenants come from
# config/tenants.dev.yaml (including bench-key) and budgets/traces/usage
# stay in RAM. That is the mode that measures *this* gateway; the dynamodb
# mode measures the whole path including the LocalStack emulator.
#
# Requires: go, hey (go install github.com/rakyll/hey@latest), and — in
# dynamodb mode — Docker LocalStack with the tables/queue created (see the
# repo README), plus Ollama at :11434 for scenario D.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-dynamodb}"

: "${AWS_ACCESS_KEY_ID:=test}"
: "${AWS_SECRET_ACCESS_KEY:=test}"
: "${AWS_REGION:=us-east-1}"
: "${AWS_ENDPOINT_URL:=http://localhost:4566}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_ENDPOINT_URL

BODY='/tmp/ai-gateway-bench-body.json'
STREAM_BODY='/tmp/ai-gateway-bench-body-stream.json'
echo '{"model":"nuva/fast","messages":[{"role":"user","content":"ping"}],"max_tokens":8}' > "$BODY"
echo '{"model":"nuva/fast","messages":[{"role":"user","content":"ping"}],"max_tokens":8,"stream":true}' > "$STREAM_BODY"

GW_URL="http://localhost:18090"
AUTH="Authorization: Bearer bench-key"
BAD_AUTH="Authorization: Bearer wrong-key-000"

cleanup() {
  [ -n "${GW_PID:-}" ] && kill "$GW_PID" 2>/dev/null || true
  [ -n "${STUB_PID:-}" ] && kill "$STUB_PID" 2>/dev/null || true
  [ -n "${STUB_SLOW_PID:-}" ] && kill "$STUB_SLOW_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "== mode: $MODE =="
echo "== building =="
go build -o /tmp/ai-gateway-bench-stub.exe ./bench/stubollama
go build -o /tmp/ai-gateway-bench-gateway.exe ./cmd/gateway

if [ "$MODE" = "memory" ]; then
  export STORE_BACKEND=memory
  # The pre-auth IP limiter would (correctly) start rejecting scenario E's
  # flood of bad keys after 60 failures; raise it so E still measures the
  # auth path rather than the limiter.
  export IP_AUTH_FAILURES_PER_MINUTE=100000000
  echo "== memory mode: tenants from config/tenants.dev.yaml, no AWS =="
else
  echo "== seeding bench tenant (bench-key, free tier, rpm 100000) =="
  go run bench/seedbench/main.go
  export IP_AUTH_FAILURES_PER_MINUTE=100000000
fi

echo "== starting stub (0 latency) on :11435 =="
/tmp/ai-gateway-bench-stub.exe -addr :11435 > /tmp/ai-gateway-bench-stub.log 2>&1 &
STUB_PID=$!

echo "== starting gateway on :18090 against stub =="
OLLAMA_BASE_URL=http://localhost:11435 GATEWAY_ADDR=:18090 /tmp/ai-gateway-bench-gateway.exe > "/tmp/ai-gateway-bench-gateway-$MODE.log" 2>&1 &
GW_PID=$!
sleep 2

run() {
  local label=$1; shift
  echo "---- $label ----"
  hey "$@"
}

echo "############ Scenario A: stub latency 0, non-stream ############"
for c in 1 10 50 100 200; do
  run "A c=$c" -z 30s -c "$c" -m POST -H "$AUTH" -H "Content-Type: application/json" -D "$BODY" "$GW_URL/v1/chat/completions"
done

echo "############ Scenario E: auth reject, c=100 ############"
run "E c=100" -z 30s -c 100 -m POST -H "$BAD_AUTH" -H "Content-Type: application/json" -D "$BODY" "$GW_URL/v1/chat/completions"

echo "############ Scenario C: stub, streaming, c=50 ############"
run "C c=50" -z 30s -c 50 -m POST -H "$AUTH" -H "Content-Type: application/json" -D "$STREAM_BODY" "$GW_URL/v1/chat/completions"

kill "$STUB_PID" 2>/dev/null || true

echo "== starting stub (200ms latency) on :11435 =="
/tmp/ai-gateway-bench-stub.exe -addr :11435 -latency 200ms > /tmp/ai-gateway-bench-stub-slow.log 2>&1 &
STUB_SLOW_PID=$!
sleep 1

echo "############ Scenario B: stub latency 200ms, non-stream ############"
for c in 50 200; do
  run "B c=$c" -z 30s -c "$c" -m POST -H "$AUTH" -H "Content-Type: application/json" -D "$BODY" "$GW_URL/v1/chat/completions"
done

kill "$STUB_SLOW_PID" 2>/dev/null || true
kill "$GW_PID" 2>/dev/null || true
sleep 1

if [ "$MODE" != "memory" ]; then
  echo "== starting gateway on :18090 against real Ollama (:11434) — scenario D =="
  OLLAMA_BASE_URL=http://localhost:11434 GATEWAY_ADDR=:18090 /tmp/ai-gateway-bench-gateway.exe > /tmp/ai-gateway-bench-gateway-ollama.log 2>&1 &
  GW_PID=$!
  sleep 2

  echo "############ Scenario D: real Ollama, non-stream ############"
  for c in 1 4; do
    run "D c=$c" -z 20s -c "$c" -m POST -H "$AUTH" -H "Content-Type: application/json" -D "$BODY" "$GW_URL/v1/chat/completions"
  done
fi

echo "== done. See docs/benchmarks/2026-09-20-local.md for the recorded numbers. =="
