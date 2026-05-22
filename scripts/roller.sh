#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

PIDFILE="state/roller.pid"
LOGFILE="logs/roller.log"
SCRIPT="scripts/roll-updown-markets.sh"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

cmd_start() {
  if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    log "roller already running (pid=$(cat "$PIDFILE"))"
    return 0
  fi
  nohup bash "$SCRIPT" >> "$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"
  log "roller started (pid=$!  log=$LOGFILE)"
}

cmd_stop() {
  if [[ -s "$PIDFILE" ]]; then
    local pid; pid=$(cat "$PIDFILE")
    kill "$pid" 2>/dev/null && log "roller stopped (pid=$pid)" || true
    rm -f "$PIDFILE"
  fi
}

cmd_status() {
  if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    log "roller RUNNING (pid=$(cat "$PIDFILE"))"
  else
    log "roller STOPPED"
  fi
  echo
  echo "=== Market files ==="
  for f in state/*.market; do
    [[ -f "$f" ]] || continue
    printf "  %-12s  %s\n" "${f#state/}" "$(sed -n '2p' "$f")"
  done
}

cmd_logs() {
  tail -f "$LOGFILE"
}

case "${1:-status}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; sleep 1; cmd_start ;;
  status)  cmd_status ;;
  logs)    cmd_logs ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
