#!/usr/bin/env bash
set -euo pipefail
ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
ROLLER="scripts/roll-updown-markets.sh"
ROLLER_PID="state/updown-roller.pid"
PORTS=(7080 7081 7082 7083 7084 7085 7086 7087 7088 7089)
GENERAL_SCRIPT="scripts/general-discovery.sh"

is_running_pid(){ local pid="${1:-}"; [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; }
roller_pid(){ [[ -s "$ROLLER_PID" ]] && cat "$ROLLER_PID" || true; }
bot_pids(){ ps -eo pid,args | awk '/[g]o run \. run --single-market .*updown-/ {print $1} /[p]olymarket-arb run --single-market .*updown-/ {print $1}'; }

start(){
  mkdir -p logs state
  local pid; pid=$(roller_pid)
  if is_running_pid "$pid"; then echo "Already running: roller pid=$pid"; status; return 0; fi
  nohup "$ROLLER" > logs/updown-roller.out 2>&1 & echo $! > "$ROLLER_PID"
  echo "Started 10-market roller pid=$(cat "$ROLLER_PID")"
  bash "$GENERAL_SCRIPT" start
  sleep 8
  status
}

stop(){
  local pid; pid=$(roller_pid)
  if is_running_pid "$pid"; then echo "Stopping roller pid=$pid"; kill "$pid" 2>/dev/null || true; fi
  bot_pids | xargs -r kill 2>/dev/null || true
  sleep 3
  bot_pids | xargs -r kill -9 2>/dev/null || true
  bash "$GENERAL_SCRIPT" stop
  rm -f "$ROLLER_PID" state/*.pid state/*.market
  echo "Stopped"
}

status(){
  echo "== config =="
  grep -E '^(EXECUTION_MODE|HTTP_PORT|ARB_MAX_PRICE_SUM|ARB_MIN_TRADE_SIZE|ARB_MAX_TRADE_SIZE|ARB_TAKER_FEE|POLYGON_RPC_URL)=' .env 2>/dev/null || true
  echo
  echo "== roller =="
  local pid; pid=$(roller_pid)
  if is_running_pid "$pid"; then ps -p "$pid" -o pid,etime,pcpu,pmem,args; else echo "roller: not running"; fi
  echo
  echo "== bot processes =="
  local pids; pids=$(bot_pids || true)
  if [[ -n "$pids" ]]; then ps -p "$(echo "$pids" | paste -sd, -)" -o pid,etime,pcpu,pmem,args || true; else echo "bot: not running"; fi
  echo
  echo "== markets =="
  for f in state/*.market; do [[ -f "$f" ]] || continue; echo "-- ${f#state/}"; cat "$f"; done
  echo
  echo "== health =="
  for p in "${PORTS[@]}"; do printf '%s ' "$p"; curl -fsS --max-time 1 "http://127.0.0.1:${p}/health" 2>/dev/null || echo -n "down"; echo; done
  printf '7090 '; curl -fsS --max-time 1 "http://127.0.0.1:7090/health" 2>/dev/null || echo -n "down (general)"; echo
  echo
  echo "== general discovery =="
  bash "$GENERAL_SCRIPT" status
  echo
  echo "== totals =="
  python3 - <<'PY'
import urllib.request, re
ports=list(range(7080,7090))+[7090]
tot={"detected":0,"received":0,"executed":0,"errors":0,"subs":0}
for p in ports:
    try:
        txt=urllib.request.urlopen(f"http://127.0.0.1:{p}/metrics",timeout=1).read().decode()
    except Exception:
        continue
    for key, metric in [("detected","polymarket_arb_opportunities_detected_total"),("received","polymarket_execution_opportunities_received_total"),("executed","polymarket_execution_opportunities_executed_total"),("errors","polymarket_execution_errors_total"),("subs","polymarket_ws_subscription_count")]:
        m=re.search(rf"^{metric} ([-0-9.]+)$", txt, re.M)
        if m: tot[key]+=float(m.group(1))
print(tot)
PY
}
case "${1:-status}" in
  start) start;;
  stop) stop;;
  restart) stop; start;;
  status) status;;
  *) echo "Usage: $0 {start|stop|restart|status}" >&2; exit 2;;
esac
