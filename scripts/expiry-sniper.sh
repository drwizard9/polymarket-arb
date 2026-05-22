#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/polymarket-arb}"
cd "$ROOT"
mkdir -p logs state

# activate virtualenv if present
if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  source "$ROOT/.venv/bin/activate"
fi

PIDFILE="state/sniper.pid"
LOGFILE="logs/sniper.log"
SCRIPT="scripts/expiry-sniper.py"

log() { printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*"; }

cmd_start() {
  if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    log "sniper already running (pid=$(cat "$PIDFILE"))"
    return 0
  fi
  # 로그 파일은 Python TimedRotatingFileHandler가 직접 관리 (rotate + gzip)
  # stderr는 별도 파일로 캡처 (크래시 원인 추적용)
  nohup python3 "$SCRIPT" > /dev/null 2>"$ROOT/logs/sniper-stderr.log" &
  echo $! > "$PIDFILE"
  log "sniper started (pid=$!  log=$LOGFILE)"
}

cmd_stop() {
  if [[ -s "$PIDFILE" ]]; then
    local pid; pid=$(cat "$PIDFILE")
    kill "$pid" 2>/dev/null && log "sniper stopped (pid=$pid)" || true
    rm -f "$PIDFILE"
  fi
}

cmd_status() {
  if [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    log "sniper RUNNING (pid=$(cat "$PIDFILE"))"
  else
    log "sniper STOPPED"
  fi
  echo
  if [[ -f logs/sniper-results.csv ]]; then
    python3 - <<'PY'
import csv, datetime, re
from pathlib import Path
from collections import defaultdict

# .env에서 현재 MODE 읽기
mode = 'live'
try:
    m = re.search(r'(?m)^MODE\s*=\s*(\S+)', Path('.env').read_text())
    if m:
        mode = m.group(1).strip().lower()
except Exception:
    pass

f = Path('logs/sniper-results.csv')
rows = list(csv.DictReader(f.open()))

# 일별 집계 (현재 MODE만)
daily = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
for r in rows:
    try:
        mode_col = r.get('', '').strip().strip("[]'\"")
        if mode not in mode_col:
            continue
        dt = datetime.datetime.strptime(r['time'], '%Y-%m-%d %H:%M:%S')
        day = dt.strftime('%Y-%m-%d')
    except Exception:
        continue
    if r['won'] == 'True':
        daily[day]['wins'] += 1
    elif r['won'] == 'False':
        daily[day]['losses'] += 1
    daily[day]['pnl'] += float(r['net_pnl'])

print(f"=== 일별 성과 (KST) [{mode}] ===")
print(f"  {'날짜':<12}  {'승':>4}  {'패':>4}  {'승률':>6}  {'손익':>8}")
print(f"  {'-'*12}  {'-'*4}  {'-'*4}  {'-'*6}  {'-'*8}")
total_w = total_l = 0
total_pnl = 0.0
for day in sorted(daily):
    d = daily[day]
    w, l = d['wins'], d['losses']
    t = w + l
    rate = w/t*100 if t else 0
    print(f"  {day}  {w:>4}  {l:>4}  {rate:>5.0f}%  ${d['pnl']:>+7.2f}")
    total_w += w; total_l += l; total_pnl += d['pnl']

total = total_w + total_l
total_rate = total_w/total*100 if total else 0
print(f"  {'합계':<12}  {total_w:>4}  {total_l:>4}  {total_rate:>5.0f}%  ${total_pnl:>+7.2f}")

PY
  else
    echo "(결과 없음)"
  fi
  echo
  _print_balance
}

_print_balance() {
  local prev_date="${1:-}"
  python3 - "$prev_date" <<'PY'
import json, urllib.request, re, sys, datetime
from pathlib import Path

prev_date = sys.argv[1] if len(sys.argv) > 1 else ''

try:
    addr = None
    try:
        env_text = open('.env').read()
        m = re.search(r'POLYMARKET_PROXY_ADDRESS\s*=\s*(\S+)', env_text)
        if m:
            addr = m.group(1).strip('"\'')
    except Exception:
        pass
    if not addr:
        addr = '0xd8dD496d8159a58d5932643200fF89b9eF61C30e'
    USDC = '0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB'
    data_hex = '0x70a08231' + addr[2:].lower().zfill(64)
    payload = json.dumps({'jsonrpc':'2.0','method':'eth_call','params':[{'to':USDC,'data':data_hex},'latest'],'id':1}).encode()
    rpcs = ['https://polygon-bor-rpc.publicnode.com','https://rpc.ankr.com/polygon','https://polygon.meowrpc.com']
    usdc = None
    for rpc in rpcs:
        try:
            req = urllib.request.Request(rpc, data=payload, headers={'Content-Type':'application/json','User-Agent':'curl/7.88'})
            resp = json.load(urllib.request.urlopen(req, timeout=5))
            if 'result' in resp and resp['result'] not in ('0x', None, '0x0'):
                usdc = int(resp['result'], 16) / 1e6
                break
        except Exception:
            continue

    # 잔고 히스토리 로드 및 저장
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    hist_path = Path('state/balance_history.json')
    history = {}
    if hist_path.exists():
        try:
            history = json.loads(hist_path.read_text())
        except Exception:
            pass
    if usdc is not None:
        history[today] = usdc
        try:
            hist_path.write_text(json.dumps(history, indent=2))
        except Exception:
            pass

    if usdc is not None:
        msg = f"  잔고: ${usdc:,.2f} USDC"
        if prev_date and prev_date in history:
            prev_bal = history[prev_date]
            diff = usdc - prev_bal
            pct = diff / prev_bal * 100 if prev_bal else 0
            sign = '+' if diff >= 0 else ''
            msg += f"  (전일: ${prev_bal:,.2f} USDC, {sign}${diff:.2f} / {sign}{pct:.1f}%)"
        print(msg)
    else:
        print(f"  잔고: 조회 실패")
except Exception as e:
    print(f"  잔고: 조회 실패 ({e})")
PY
}

cmd_daily() {
  # 특정 날짜 조회: bash expiry-sniper.sh daily [YYYY-MM-DD]
  local target="${1:-$(TZ=Asia/Seoul date '+%Y-%m-%d')}"
  local prev_date
  prev_date=$(TZ=Asia/Seoul date -d "$target - 1 day" '+%Y-%m-%d' 2>/dev/null || echo "")
  if [[ ! -f logs/sniper-results.csv ]]; then
    echo "(결과 없음)"; return
  fi
  python3 - "$target" <<'PY'
import csv, datetime, sys, re
from pathlib import Path

target = sys.argv[1]

# .env에서 현재 MODE 읽기
mode = 'live'
try:
    m = re.search(r'(?m)^MODE\s*=\s*(\S+)', Path('.env').read_text())
    if m:
        mode = m.group(1).strip().lower()
except Exception:
    pass

rows = list(csv.DictReader(Path('logs/sniper-results.csv').open()))
day_rows = []
for r in rows:
    try:
        dt = datetime.datetime.strptime(r['time'], '%Y-%m-%d %H:%M:%S')
        if dt.strftime('%Y-%m-%d') != target:
            continue
        mode_col = r.get('', '').strip().strip("[]'\"")
        if mode not in mode_col:
            continue
        day_rows.append((dt, r))
    except Exception:
        continue

if not day_rows:
    print(f"  {target} 거래 없음 (mode={mode})")
    sys.exit(0)

print(f"=== {target} KST 상세 [{mode}] ===")
print(f"  {'시각(KST)':<8}  {'종목':<6}  {'방향':<5}  {'ask':>5}  {'결과':<4}  {'손익':>7}")
print(f"  {'-'*8}  {'-'*6}  {'-'*5}  {'-'*5}  {'-'*4}  {'-'*7}")
wins = losses = 0
pnl = 0.0
for dt, r in day_rows:
    coin = next((c for c in ['btc','eth','sol','bnb','xrp','doge','hype'] if c in r['slug']), '?')
    iv = '15m' if '15m' in r['slug'] else '5m'
    result = '✅WIN' if r['won']=='True' else '❌LOSS'
    if r['won']=='True': wins+=1
    else: losses+=1
    pnl += float(r['net_pnl'])
    print(f"  {dt.strftime('%H:%M:%S')}  {coin.upper():<4}/{iv}  {r['bet_outcome']:<5}  {float(r['ask']):>5.2f}  {result}  ${float(r['net_pnl']):>+6.2f}")

total = wins+losses
rate = wins/total*100 if total else 0
print(f"\n  합계: {wins}승 {losses}패  승률: {rate:.0f}%  손익: ${pnl:+.2f}")
PY
  echo
  _print_balance "$prev_date"
}

cmd_monthly() {
  # 월간 조회: bash expiry-sniper.sh monthly [YYYY-MM]
  local target="${1:-$(TZ=Asia/Seoul date '+%Y-%m')}"
  if [[ ! -f logs/sniper-results.csv ]]; then
    echo "(결과 없음)"; return
  fi
  python3 - "$target" <<'PY'
import csv, datetime, sys, re
from pathlib import Path
from collections import defaultdict

target = sys.argv[1]

# .env에서 현재 MODE 읽기
mode = 'live'
try:
    m = re.search(r'(?m)^MODE\s*=\s*(\S+)', Path('.env').read_text())
    if m:
        mode = m.group(1).strip().lower()
except Exception:
    pass

rows = list(csv.DictReader(Path('logs/sniper-results.csv').open()))

daily = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl': 0.0})
for r in rows:
    try:
        mode_col = r.get('', '').strip().strip("[]'\"")
        if mode not in mode_col:
            continue
        dt = datetime.datetime.strptime(r['time'], '%Y-%m-%d %H:%M:%S')
        if dt.strftime('%Y-%m') != target:
            continue
        day = dt.strftime('%Y-%m-%d')
    except Exception:
        continue
    if r['won'] == 'True':
        daily[day]['wins'] += 1
    elif r['won'] == 'False':
        daily[day]['losses'] += 1
    daily[day]['pnl'] += float(r['net_pnl'])

if not daily:
    print(f"  {target} 거래 없음")
    sys.exit(0)

print(f"=== {target} 월간 성과 (KST) [{mode}] ===")
print(f"  {'날짜':<12}  {'승':>4}  {'패':>4}  {'승률':>6}  {'손익':>8}")
print(f"  {'-'*12}  {'-'*4}  {'-'*4}  {'-'*6}  {'-'*8}")
total_w = total_l = 0
total_pnl = 0.0
for day in sorted(daily):
    d = daily[day]
    w, l = d['wins'], d['losses']
    t = w + l
    rate = w/t*100 if t else 0
    print(f"  {day}  {w:>4}  {l:>4}  {rate:>5.0f}%  ${d['pnl']:>+7.2f}")
    total_w += w; total_l += l; total_pnl += d['pnl']

total = total_w + total_l
total_rate = total_w/total*100 if total else 0
print(f"  {'합계':<12}  {total_w:>4}  {total_l:>4}  {total_rate:>5.0f}%  ${total_pnl:>+7.2f}")
PY
  echo
  _print_balance
}

cmd_logs() {
  tail -f "$LOGFILE"
}

cmd_reset() {
  cmd_stop
  echo "time,slug,question,bet_outcome,ask,implied_pct,fee,winner,won,net_pnl" > logs/sniper-results.csv
  printf '\n=== %s | stats reset ===\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" >> "$LOGFILE"
  log "stats reset"
  sleep 1
  cmd_start
}

case "${1:-status}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; sleep 1; cmd_start ;;
  status)  cmd_status ;;
  logs)    cmd_logs ;;
  reset)   cmd_reset ;;
  daily)   cmd_daily "${2:-}" ;;
  monthly) cmd_monthly "${2:-}" ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs|reset|daily [YYYY-MM-DD]|monthly [YYYY-MM]}"
    exit 1
    ;;
esac
