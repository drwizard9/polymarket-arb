#!/usr/bin/env bash
set -uo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

CHECK_INTERVAL="${WATCHDOG_INTERVAL:-60}"  # 기본 60초마다 점검

log() { printf '%s [watchdog] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a logs/watchdog.log; }

# ── 중복 프로세스 제거: 패턴에 맞는 프로세스가 2개 이상이면 최신 하나만 남김 ──
dedup() {
    local pattern="$1" label="$2"
    local pids
    pids=$(pgrep -f "$pattern" | grep -v "^$$\$" || true)
    local count
    count=$(echo "$pids" | grep -c '[0-9]' || true)
    if [[ "$count" -gt 1 ]]; then
        log "WARN  $label 중복 실행 감지 (${count}개) — 구버전 종료"
        # 가장 최근 PID(마지막)만 남기고 나머지 종료
        local newest
        newest=$(echo "$pids" | sort -n | tail -1)
        echo "$pids" | sort -n | head -n -1 | xargs -r kill 2>/dev/null || true
        log "INFO  $label 중복 제거 완료 — 유지 PID=$newest"
    fi
}

# ── 프로세스 헬스체크 및 재시작 ──
ensure_roller() {
    local pidfile="state/roller.pid"
    dedup "roll-updown-markets.sh" "roller"

    if [[ -s "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        return 0  # 정상 실행 중
    fi

    # PID 파일 없거나 죽어있으면 pgrep으로 재확인
    local pid
    pid=$(pgrep -f "roll-updown-markets.sh" | head -1 || true)
    if [[ -n "$pid" ]]; then
        echo "$pid" > "$pidfile"
        return 0  # 실행 중이지만 PID 파일만 없었던 경우
    fi

    log "RESTART  roller 프로세스 없음 — 재시작"
    bash scripts/roller.sh start
}

ensure_sniper() {
    local pidfile="state/sniper.pid"
    dedup "expiry-sniper.py" "sniper"

    if [[ -s "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        return 0  # 정상 실행 중
    fi

    local pid
    pid=$(pgrep -f "expiry-sniper.py" | head -1 || true)
    if [[ -n "$pid" ]]; then
        echo "$pid" > "$pidfile"
        return 0
    fi

    log "RESTART  sniper 프로세스 없음 — 재시작"
    bash scripts/expiry-sniper.sh start
}

# ── 감시 루프 ──
log "watchdog 시작 (점검 주기=${CHECK_INTERVAL}s)"

while true; do
    ensure_roller
    ensure_sniper
    sleep "$CHECK_INTERVAL"
done
