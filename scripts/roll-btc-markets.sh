#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

# source .env for bot/runtime defaults
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

INTERVAL="${INTERVAL:-30}"  # fallback/error retry interval
ROLL_LEAD_SECONDS="${ROLL_LEAD_SECONDS:-45}"
ARB_MAX_PRICE_SUM="${ARB_MAX_PRICE_SUM:-0.997}"
ARB_MIN_TRADE_SIZE="${ARB_MIN_TRADE_SIZE:-1.0}"
ARB_MAX_TRADE_SIZE="${ARB_MAX_TRADE_SIZE:-2.0}"
ARB_TAKER_FEE="${ARB_TAKER_FEE:-0.0000}"
WS_POOL_SIZE="${WS_POOL_SIZE:-1}"
WATCH_15M="${WATCH_15M:-0}"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a logs/btc-roller.log; }

find_market() {
  local kind="$1"
  python3 - "$kind" <<'PYMARKET'
import json, urllib.request, datetime, sys, time
kind=sys.argv[1]
if kind == "5m":
    prefix="btc-updown-5m-"
    step=300
elif kind == "15m":
    prefix="btc-updown-15m-"
    step=900
else:
    raise SystemExit(2)

headers={"User-Agent":"polymarket-arb/1.0","Accept":"application/json"}
now_ts=int(time.time())
now=datetime.datetime.now(datetime.timezone.utc)
min_end=now+datetime.timedelta(seconds=45)
base=(now_ts//step)*step
candidates=[]
# Try current interval and near-future intervals by deterministic slug.
for ts in range(base-step, base+(step*8), step):
    if ts <= 0:
        continue
    slug=f"{prefix}{ts}"
    url=f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1"
    try:
        req=urllib.request.Request(url, headers=headers)
        data=json.load(urllib.request.urlopen(req, timeout=10))
    except Exception:
        continue
    if not data:
        continue
    m=data[0]
    if m.get("slug") != slug:
        continue
    try:
        end=datetime.datetime.fromisoformat((m.get("endDate") or "").replace("Z","+00:00"))
    except Exception:
        continue
    if end <= min_end:
        continue
    if not m.get("active") or m.get("closed"):
        continue
    if m.get("acceptingOrders") is False:
        continue
    q=m.get("question") or ""
    candidates.append((end, ts, slug, q))

if not candidates:
    raise SystemExit(2)
end, ts, slug, q=sorted(candidates, key=lambda x:(x[0], x[1]))[0]
print(slug)
print(q)
print(end.isoformat())
PYMARKET
}

kill_kind() {
  local kind="$1" port="$2"
  local pidfile="state/btc-${kind}.pid"
  if [[ -s "$pidfile" ]]; then
    local pid
    pid=$(cat "$pidfile" || true)
    if [[ -n "${pid:-}" ]]; then kill "$pid" 2>/dev/null || true; fi
  fi
  # Kill go-run parent and compiled child for this kind's current/old single-market watcher.
  pkill -f "go run \\. run --single-market btc-updown-${kind}-" 2>/dev/null || true
  pkill -f "polymarket-arb run --single-market btc-updown-${kind}-" 2>/dev/null || true
  # If something still owns the port, remove it. Prefer lsof when present.
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
  fi
  sleep 1
}

start_kind() {
  local kind="$1" port="$2" slug="$3" question="$4" end="$5"
  kill_kind "$kind" "$port"
  local logfile="logs/btc-${kind}.log"
  printf '\n=== %s | %s | ends %s ===\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$slug" "$end" >> "$logfile"
  log "starting btc-${kind}: $slug | $question | ends $end | port $port"
  nohup env HTTP_PORT="$port" "${ROOT:-$HOME/polymarket-arb}/polymarket-arb" run --single-market "$slug" >> "$logfile" 2>&1 &
  echo $! > "state/btc-${kind}.pid"
  printf '%s\n%s\n%s\n' "$slug" "$question" "$end" > "state/btc-${kind}.market"
}

health_ok() {
  local port="$1"
  curl -fsS --max-time 2 "http://127.0.0.1:${port}/health" >/dev/null 2>&1
}

ensure_kind() {
  local kind="$1" port="$2"
  local info slug question end current=""
  if ! info=$(find_market "$kind" 2>>logs/btc-roller.log); then
    log "WARN: no active btc-${kind} market found"
    return 0
  fi
  slug=$(printf '%s\n' "$info" | sed -n '1p')
  question=$(printf '%s\n' "$info" | sed -n '2p')
  end=$(printf '%s\n' "$info" | sed -n '3p')
  [[ -f "state/btc-${kind}.market" ]] && current=$(sed -n '1p' "state/btc-${kind}.market" || true)
  if [[ "$slug" != "$current" ]] || ! health_ok "$port"; then
    if [[ "$slug" != "$current" ]]; then
      log "rolling btc-${kind}: ${current:-none} -> $slug"
    else
      log "restarting btc-${kind}: health check failed on port $port"
    fi
    start_kind "$kind" "$port" "$slug" "$question" "$end"
  fi
}

next_sleep_seconds() {
  local market_file="state/btc-5m.market"
  if [[ ! -f "$market_file" ]]; then
    echo "$INTERVAL"
    return 0
  fi

  local end
  end=$(sed -n '3p' "$market_file" || true)
  if [[ -z "${end:-}" ]]; then
    echo "$INTERVAL"
    return 0
  fi

  python3 - "$end" "$ROLL_LEAD_SECONDS" "$INTERVAL" <<'PYSLEEP'
import datetime, sys
end_raw=sys.argv[1]
lead=int(sys.argv[2])
fallback=int(sys.argv[3])
try:
    end=datetime.datetime.fromisoformat(end_raw.replace('Z','+00:00'))
    now=datetime.datetime.now(datetime.timezone.utc)
    target=end-datetime.timedelta(seconds=lead)
    seconds=int((target-now).total_seconds())
except Exception:
    seconds=fallback
# If already close to rollover, retry soon. Cap long sleeps so health is still checked occasionally.
if seconds < 5:
    seconds=5
elif seconds > 240:
    seconds=240
print(seconds)
PYSLEEP
}

log "btc roller started mode=end-time roll_lead=${ROLL_LEAD_SECONDS}s fallback_interval=${INTERVAL}s threshold=${ARB_MAX_PRICE_SUM} watch_15m=${WATCH_15M}"
while true; do
  ensure_kind 5m 7080
  if [[ "$WATCH_15M" == "1" ]]; then
    ensure_kind 15m 8081
  else
    kill_kind 15m 8081
  fi
  sleep_for=$(next_sleep_seconds)
  log "next btc-5m check in ${sleep_for}s"
  sleep "$sleep_for"
done
