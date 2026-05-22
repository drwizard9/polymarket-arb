#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

PORT=7090
PIDFILE="state/general.pid"
LOGFILE="logs/general.log"
BINARY="$ROOT/polymarket-arb"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

health_ok() {
  curl -fsS --max-time 2 "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1
}

cmd_start() {
  if [[ -s "$PIDFILE" ]]; then
    local pid
    pid=$(cat "$PIDFILE")
    if kill -0 "$pid" 2>/dev/null && health_ok; then
      log "general discovery already running (pid=$pid, port=$PORT)"
      return 0
    fi
  fi

  if ! [[ -x "$BINARY" ]]; then
    log "ERROR: binary not found at $BINARY — run: go build -o polymarket-arb ."
    exit 1
  fi

  printf '\n=== %s | general discovery started ===\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" >> "$LOGFILE"
  log "starting general discovery on port $PORT (ARB_MAX_MARKET_DURATION=${ARB_MAX_MARKET_DURATION:-unset})"
  nohup env HTTP_PORT="$PORT" "$BINARY" run >> "$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"
  log "general discovery started (pid=$!, log=$LOGFILE)"
}

cmd_stop() {
  if [[ -s "$PIDFILE" ]]; then
    local pid
    pid=$(cat "$PIDFILE")
    kill "$pid" 2>/dev/null && log "stopped general discovery (pid=$pid)" || true
    rm -f "$PIDFILE"
  fi
  pkill -f "$BINARY run$" 2>/dev/null || true
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | xargs -r kill 2>/dev/null || true
  fi
}

cmd_status() {
  local pid=""
  if [[ -s "$PIDFILE" ]]; then
    pid=$(cat "$PIDFILE")
  fi
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    if health_ok; then
      log "general discovery RUNNING (pid=$pid, port=$PORT) ✓"
    else
      log "general discovery RUNNING (pid=$pid) but health check FAILED on port $PORT"
    fi
  else
    log "general discovery STOPPED"
  fi
}

cmd_restart() {
  cmd_stop
  sleep 1
  cmd_start
}

cmd_logs() {
  tail -f "$LOGFILE"
}

case "${1:-status}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_restart ;;
  status)  cmd_status ;;
  logs)    cmd_logs ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
