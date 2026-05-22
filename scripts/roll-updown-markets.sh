#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

# Load base settings. Per-process HTTP_PORT is intentionally overridden below.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

INTERVAL="${INTERVAL:-30}"
ROLL_LEAD_SECONDS="${ROLL_LEAD_SECONDS:-45}"

# 14 live watchers: BTC, ETH, SOL, BNB, XRP, DOGE, HYPE × 5m + 15m
# name|symbol|interval|seconds|port
TARGETS=(
  "btc5m|btc|5m|300|7080"
  "eth5m|eth|5m|300|7081"
  "sol5m|sol|5m|300|7082"
  "bnb5m|bnb|5m|300|7083"
  "btc15m|btc|15m|900|7084"
  "eth15m|eth|15m|900|7085"
  "sol15m|sol|15m|900|7086"
  "bnb15m|bnb|15m|900|7087"
  "xrp5m|xrp|5m|300|7088"
  "doge5m|doge|5m|300|7089"
  "xrp15m|xrp|15m|900|7090"
  "doge15m|doge|15m|900|7091"
  "hype5m|hype|5m|300|7092"
  "hype15m|hype|15m|900|7093"
)

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a logs/updown-roller.log; }

find_market() {
  local symbol="$1" interval="$2" step="$3"
  python3 - "$symbol" "$interval" "$step" "$ROLL_LEAD_SECONDS" <<'PY'
import json, urllib.request, datetime, sys, time
symbol, interval, step, lead = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
prefix=f"{symbol}-updown-{interval}-"
headers={"User-Agent":"polymarket-arb/1.0","Accept":"application/json"}
now_ts=int(time.time())
now=datetime.datetime.now(datetime.timezone.utc)
min_end=now+datetime.timedelta(seconds=lead)
base=(now_ts//step)*step
candidates=[]
for ts in range(base-step, base+(step*8), step):
    if ts <= 0: continue
    slug=f"{prefix}{ts}"
    url=f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1"
    try:
        req=urllib.request.Request(url, headers=headers)
        data=json.load(urllib.request.urlopen(req, timeout=10))
    except Exception:
        continue
    if not data: continue
    m=data[0]
    if m.get("slug") != slug: continue
    try:
        end=datetime.datetime.fromisoformat((m.get("endDate") or "").replace("Z","+00:00"))
    except Exception:
        continue
    if end <= min_end: continue
    if not m.get("active") or m.get("closed"): continue
    if m.get("acceptingOrders") is False: continue
    candidates.append((end, ts, slug, m.get("question") or ""))
if not candidates:
    raise SystemExit(2)
end, ts, slug, question = sorted(candidates, key=lambda x:(x[0], x[1]))[0]
print(slug)
print(question)
print(end.isoformat())
PY
}

pids_for_target() {
  local name="$1" slug_prefix="$2"
  ps -eo pid,args | awk -v pfx="$slug_prefix" '/[g]o run \. run --single-market/ && index($0,pfx) {print $1} /[p]olymarket-arb run --single-market/ && index($0,pfx) {print $1}'
}

kill_target() {
  local name="$1" symbol="$2" interval="$3" port="$4"
  local pidfile="state/${name}.pid"
  if [[ -s "$pidfile" ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
  fi
  pids_for_target "$name" "${symbol}-updown-${interval}-" | xargs -r kill 2>/dev/null || true
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
  fi
}

health_ok() {
  local port="$1"
  curl -fsS --max-time 2 "http://127.0.0.1:${port}/health" >/dev/null 2>&1
}

start_target() {
  local name="$1" symbol="$2" interval="$3" port="$4" slug="$5" question="$6" end="$7"
  kill_target "$name" "$symbol" "$interval" "$port"
  sleep 1
  log "starting ${name}: ${slug} | ${question} | ends ${end} | port ${port}"
  nohup env HTTP_PORT="$port" "$ROOT/polymarket-arb" run --single-market "$slug" >> /dev/null 2>&1 &
  echo $! > "state/${name}.pid"
  printf '%s\n%s\n%s\n%s\n' "$slug" "$question" "$end" "$port" > "state/${name}.market"
}

ensure_target() {
  local spec="$1"
  IFS='|' read -r name symbol interval step port <<< "$spec"
  local info slug question end current=""
  if ! info=$(find_market "$symbol" "$interval" "$step" 2>>logs/updown-roller.log); then
    log "WARN: no active market for ${name} (${symbol} ${interval})"
    return 0
  fi
  slug=$(printf '%s\n' "$info" | sed -n '1p')
  question=$(printf '%s\n' "$info" | sed -n '2p')
  end=$(printf '%s\n' "$info" | sed -n '3p')
  [[ -f "state/${name}.market" ]] && current=$(sed -n '1p' "state/${name}.market" || true)
  if [[ "$slug" != "$current" ]] || ! health_ok "$port"; then
    if [[ "$slug" != "$current" ]]; then
      log "rolling ${name}: ${current:-none} -> ${slug}"
    else
      log "restarting ${name}: health failed on port ${port}"
    fi
    start_target "$name" "$symbol" "$interval" "$port" "$slug" "$question" "$end"
  fi
}

stop_all() {
  for spec in "${TARGETS[@]}"; do
    IFS='|' read -r name symbol interval step port <<< "$spec"
    kill_target "$name" "$symbol" "$interval" "$port"
  done
}

trap stop_all EXIT
log "updown roller started targets=${#TARGETS[@]} (btc,eth,sol,bnb,xrp,doge,hype × 5m+15m) interval=${INTERVAL}s threshold=${ARB_MAX_PRICE_SUM:-unset} mode=${EXECUTION_MODE:-unset}"
while true; do
  for spec in "${TARGETS[@]}"; do
    ensure_target "$spec"
  done
  sleep "$INTERVAL"
done
