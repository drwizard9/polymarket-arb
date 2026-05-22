#!/usr/bin/env bash
set -uo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

PIDFILE="state/watchdog.pid"
LOGFILE="logs/watchdog.log"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

cmd_start() {
    if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        log "watchdog already running (pid=$(cat "$PIDFILE"))"
        return 0
    fi
    nohup bash scripts/watchdog.sh >> "$LOGFILE" 2>&1 &
    echo $! > "$PIDFILE"
    log "watchdog started (pid=$!  log=$LOGFILE)"
}

cmd_stop() {
    if [[ -s "$PIDFILE" ]]; then
        local pid; pid=$(cat "$PIDFILE")
        kill "$pid" 2>/dev/null && log "watchdog stopped (pid=$pid)" || true
        rm -f "$PIDFILE"
    fi
}

cmd_status() {
    if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        log "watchdog RUNNING (pid=$(cat "$PIDFILE"))"
    else
        log "watchdog STOPPED"
    fi
    echo
    echo "=== 최근 감시 로그 ==="
    [[ -f "$LOGFILE" ]] && tail -10 "$LOGFILE" || echo "(로그 없음)"
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
