#!/usr/bin/env python3
"""
Expiry Sniper — Directional betting near market expiry.
Supports paper mode (simulation) and live mode (real orders via CLOB API).
Stop-loss: auto-sell if bid drops below STOP_LOSS_PCT of entry ask.
"""
import json, time, datetime, urllib.request, os, csv, xml.etree.ElementTree as ET, threading, subprocess, sqlite3, math, logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / '.env', override=True)

ROOT              = Path(os.environ.get('ROOT', Path.home() / 'polymarket-arb'))
STATE_DIR         = ROOT / 'state'
RESULTS_CSV       = ROOT / 'logs' / 'sniper-results.csv'
ML_DB             = ROOT / 'logs' / 'sniper-ml.db'
PENDING_BETS_FILE = STATE_DIR / 'pending_bets.json'
BET_SIZE      = float(os.environ.get('BET_SIZE',      '5.0'))
TRIGGER       = int(os.environ.get('TRIGGER_SECS',   '120'))  # 전역 fallback
MIN_ENTRY_SECS = int(os.environ.get('MIN_ENTRY_SECS', '30'))  # 마켓 종료 N초 전부터 진입 금지

def get_trigger(coin: str, interval: str) -> int:
    """TRIGGER 조회 우선순위: {COIN}_{IV}_TRIGGER > TRIGGER_{IV} > {IV}_TRIGGER > TRIGGER_SECS
    예: BTC_5M_TRIGGER=120 > TRIGGER_5M=150 > 5M_TRIGGER=150 > TRIGGER_SECS=240
    참고: bash 호환을 위해 TRIGGER_5M 형식 권장 (5M_TRIGGER는 숫자 시작으로 bash source 불가)"""
    iv_key = interval.upper().replace('M', 'M')  # '5m' → '5M'
    coin_iv_key = f"{coin.upper()}_{iv_key}_TRIGGER"
    val = (os.environ.get(coin_iv_key)
           or os.environ.get(f"TRIGGER_{iv_key}")   # TRIGGER_5M (bash 호환)
           or os.environ.get(f"{iv_key}_TRIGGER"))  # 5M_TRIGGER (구버전 호환)
    return int(val) if val else TRIGGER
FEE_RATE      = float(os.environ.get('FEE_RATE',      '0.0175'))
MIN_NET_PNL   = float(os.environ.get('MIN_NET_PNL',   '0.05'))
MIN_ASK          = float(os.environ.get('MIN_ASK',          '0.83'))  # ask 하한 (전역)
MAX_ASK          = float(os.environ.get('MAX_ASK',          '1.00'))  # ask 상한 (전역)
STOP_LOSS_PCT        = float(os.environ.get('STOP_LOSS_PCT',        '0.50'))  # bid < ask*PCT 시 손절
STOP_LOSS_FLOOR_SECS = int(os.environ.get('STOP_LOSS_FLOOR_SECS',  '60'))   # 마감 N초 이내는 손절 안 함
TAKE_PROFIT_TRAIL    = float(os.environ.get('TAKE_PROFIT_TRAIL',    '0.05')) # 익절 트레일링: peak에서 N% 하락 시 익절
CONSEC_LOSS_ALERT = int(os.environ.get('CONSEC_LOSS_ALERT', '3'))  # N연패 텔레그램 경고
# ── 급반전 방어 ──────────────────────────────────────────────────────────
CONSEC_LOSS_COOLDOWN      = int(os.environ.get('CONSEC_LOSS_COOLDOWN',      '2'))    # 방법1: N연패 시 전체 쿨다운
CONSEC_LOSS_COOLDOWN_SECS = int(os.environ.get('CONSEC_LOSS_COOLDOWN_SECS', '300'))  # 방법1: 쿨다운 기간(초)
COIN_LOSS_COOLDOWN_SECS   = int(os.environ.get('COIN_LOSS_COOLDOWN_SECS',   '600'))  # 방법2: 코인별 손실 후 대기(초)
MAX_VOLATILITY_PCT        = float(os.environ.get('MAX_VOLATILITY_PCT',      '0.60')) # 방법4: 5분 변동성 상한(%)
MOMENTUM_MINS      = int(os.environ.get('MOMENTUM_MINS',      '5'))     # 모멘텀 측정 구간(분)
MIN_R2             = float(os.environ.get('MIN_R2',            '0.7'))   # 선형회귀 R² 최소값 (추세 일관성)
MIN_SLOPE_PCT      = float(os.environ.get('MIN_SLOPE_PCT',     '0.03'))  # 분당 최소 slope (%)
# BTC는 독립 진입, ETH/SOL은 알트 그룹 (방향 제한 없음 — 둘 다 동시 진입 허용)
_ALT_GROUP = {'eth', 'bnb', 'sol'}
# 정렬 우선순위 (낮을수록 먼저, BTC는 -1 = 항상 독립)
_COIN_ENTER_PRIORITY = {'eth': 0, 'bnb': 1, 'sol': 2, 'btc': -1}
MODE          = os.environ.get('MODE', 'paper').lower()  # paper | live
ALLOWED_COINS = [c.lower() for c in os.environ.get('ALLOWED_COINS', 'btc,eth,sol').split(',')]
PAPER_CAPITAL = float(os.environ.get('PAPER_CAPITAL', '0'))  # paper mode 초기 자본금 (0=미설정)

# ── Phase 1 catastrophic blocks (2026-05-19, drwizard 6390) ─────────────
# 5/18 -$94 분석: 09:23 4 trades 동시 진입 (BTC/ETH/SOL/XRP all Up) → correlated 손실.
# B-1 백테스트 (254 trades 9일): WR 88% 인데 PnL -$103 (mean win $0.74 / mean loss $8.96).
MAX_CONCURRENT_BETS       = int(os.environ.get('MAX_CONCURRENT_BETS',       '2'))   # 동시 활성 bet 최대 (0=무제한)
CORRELATED_SAME_DIR_LIMIT = int(os.environ.get('CORRELATED_SAME_DIR_LIMIT', '1'))   # 동일 방향(Up/Down) 동시 진입 최대 (0=무제한)
DAILY_LOSS_CAP_USD        = float(os.environ.get('DAILY_LOSS_CAP_USD',      '20.0')) # 일일 손실 도달 시 신규 진입 차단 (0=비활성, KST 자정 자동 리셋)

def coin_cfg(coin: str, key: str, default: float) -> float:
    """코인별 설정값 조회. {COIN}_{KEY} 환경변수 우선, 없으면 전역값 사용."""
    return float(os.environ.get(f"{coin.upper()}_{key}", default))
INTERVAL      = 5    # polling interval seconds
GIVE_UP       = 900  # stop checking 15 minutes post-expiry

# Blackout windows in ET time (HH:MM/duration_minutes).
_BLACKOUT_RAW = os.environ.get('BLACKOUT_WINDOWS_ET', '08:00/90')
BLACKOUT_WINDOWS = []
for _spec in _BLACKOUT_RAW.split(','):
    _spec = _spec.strip()
    if not _spec:
        continue
    try:
        _t, _d = _spec.split('/')
        _h, _m = map(int, _t.split(':'))
        BLACKOUT_WINDOWS.append((_h, _m, int(_d)))
    except Exception:
        pass

ECO_BLACKOUT_MINS = int(os.environ.get('ECO_BLACKOUT_MINS', '30'))
ECO_IMPACT        = os.environ.get('ECO_IMPACT', 'High').split(',')

# Telegram
_TG_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
_TG_CHAT  = os.environ.get('TELEGRAM_CHAT_ID', '')

_ENV_FILE = Path(__file__).parent.parent / '.env'

def _save_env_key(key: str, value: str):
    """기존 .env에서 key=value 줄을 업데이트하거나 없으면 추가."""
    try:
        text = _ENV_FILE.read_text()
        import re
        pattern = rf'^{re.escape(key)}=.*$'
        new_line = f'{key}={value}'
        if re.search(pattern, text, flags=re.MULTILINE):
            text = re.sub(pattern, new_line, text, flags=re.MULTILINE)
        else:
            text = text.rstrip('\n') + f'\n{new_line}\n'
        _ENV_FILE.write_text(text)
    except Exception as e:
        log(f"WARN  .env 저장 실패: {e}")

# /set 명령으로 변경 가능한 키 테이블
# (env_key, runtime_global_or_None, type)
# runtime_global=None 인 경우 os.environ 만 업데이트 (coin_cfg가 런타임에 읽음)
_SETTABLE: dict = {
    'MIN_ASK':              ('MIN_ASK',          float),
    'MAX_ASK':              ('MAX_ASK',          float),
    'MOMENTUM_MINS':        ('MOMENTUM_MINS',    int),
    'TRIGGER_SECS':         ('TRIGGER',          int),   # 전역 fallback
    'MIN_ENTRY_SECS':       ('MIN_ENTRY_SECS',   int),   # 종료 N초 전 진입 금지
    '5M_TRIGGER':           (None,               int),   # 5m 마켓 전용
    '15M_TRIGGER':          (None,               int),   # 15m 마켓 전용
    'BTC_5M_TRIGGER':       (None,               int),
    'BTC_15M_TRIGGER':      (None,               int),
    'ETH_5M_TRIGGER':       (None,               int),
    'ETH_15M_TRIGGER':      (None,               int),
    'SOL_5M_TRIGGER':       (None,               int),
    'SOL_15M_TRIGGER':      (None,               int),
    'XRP_5M_TRIGGER':       (None,               int),
    'XRP_15M_TRIGGER':      (None,               int),
    'BNB_5M_TRIGGER':       (None,               int),
    'BNB_15M_TRIGGER':      (None,               int),
    'DOGE_5M_TRIGGER':      (None,               int),
    'DOGE_15M_TRIGGER':     (None,               int),
    'HYPE_5M_TRIGGER':      (None,               int),
    'HYPE_15M_TRIGGER':     (None,               int),
    'BET_SIZE':             ('BET_SIZE',         float),
    'BTC_MIN_ASK':          (None,               float),
    'ETH_MIN_ASK':          (None,               float),
    'SOL_MIN_ASK':          (None,               float),
    'XRP_MIN_ASK':          (None,               float),
    'BNB_MIN_ASK':          (None,               float),
    'DOGE_MIN_ASK':         (None,               float),
    'HYPE_MIN_ASK':         (None,               float),
    'BTC_BET_SIZE':         (None,               float),
    'ETH_BET_SIZE':         (None,               float),
    'SOL_BET_SIZE':         (None,               float),
    'XRP_BET_SIZE':         (None,               float),
    'BNB_BET_SIZE':         (None,               float),
    'DOGE_BET_SIZE':        (None,               float),
    'HYPE_BET_SIZE':        (None,               float),
    'CONSEC_LOSS_ALERT':    ('CONSEC_LOSS_ALERT', int),
}

def tg_set_value(env_key: str, raw_val: str) -> str:
    """값을 런타임 + .env + os.environ 에 반영. 결과 메시지 반환."""
    env_key = env_key.upper()
    if env_key not in _SETTABLE:
        keys = ', '.join(_SETTABLE)
        return f"❌ 알 수 없는 키: {env_key}\n설정 가능: {keys}"
    runtime_var, val_type = _SETTABLE[env_key]
    try:
        typed_val = val_type(raw_val)
    except ValueError:
        return f"❌ 값 오류: {raw_val!r} — {'정수' if val_type is int else '실수'}여야 합니다"
    # runtime global 업데이트
    if runtime_var:
        globals()[runtime_var] = typed_val
    # os.environ 업데이트 (coin_cfg 런타임 반영)
    os.environ[env_key] = str(typed_val)
    # .env 파일 저장
    _save_env_key(env_key, str(typed_val))
    log(f"TG-CMD  /set {env_key}={typed_val} (.env 저장)")
    return f"✅ <b>{env_key}</b> = {typed_val}"

def send_telegram(text: str):
    if not _TG_TOKEN or not _TG_CHAT:
        return
    try:
        payload = json.dumps({'chat_id': _TG_CHAT, 'text': text, 'parse_mode': 'HTML'}).encode()
        req = urllib.request.Request(
            f'https://api.telegram.org/bot{_TG_TOKEN}/sendMessage',
            data=payload, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        # HTML 파싱 오류(400) 시 plain text 재시도
        if '400' in str(e):
            try:
                payload2 = json.dumps({'chat_id': _TG_CHAT, 'text': text.replace('<b>','').replace('</b>','').replace('<i>','').replace('</i>','')}).encode()
                req2 = urllib.request.Request(
                    f'https://api.telegram.org/bot{_TG_TOKEN}/sendMessage',
                    data=payload2, headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req2, timeout=5)
            except Exception as e2:
                log(f"WARN  Telegram 전송 실패: {e2}")
        else:
            log(f"WARN  Telegram 전송 실패: {e}")

# 코인별 허용 interval 런타임 테이블 (빈 set = 제한 없음)
# .env의 {COIN}_INTERVALS=5m,15m 으로 초기값 설정, /add /del 명령으로 런타임 변경 가능
_coin_interval_filter: dict = {}
for _c in ALLOWED_COINS:
    _raw_iv = os.environ.get(f"{_c.upper()}_INTERVALS", '')
    _coin_interval_filter[_c] = set(x.strip() for x in _raw_iv.split(',') if x.strip())

def coin_intervals(coin: str) -> set:
    return _coin_interval_filter.get(coin, set())

def et_now():
    utc = datetime.datetime.now(datetime.timezone.utc)
    offset = -4 if 3 <= utc.month <= 11 else -5
    return utc + datetime.timedelta(hours=offset)

# ── Economic calendar (ForexFactory) ─────────────────────────────────────────
_eco_cache      = []
_eco_cache_date = None

def _fetch_ff_calendar(week='thisweek'):
    url = f'https://nfs.faireconomy.media/ff_calendar_{week}.xml'
    req = urllib.request.Request(url, headers={'User-Agent': 'expiry-sniper/1.0'})
    raw = urllib.request.urlopen(req, timeout=10).read()
    return ET.fromstring(raw)

def _parse_et_time(date_str, time_str):
    if not time_str or time_str.lower() in ('all day', 'tentative', ''):
        return None
    try:
        return datetime.datetime.strptime(f"{date_str} {time_str.upper()}", '%m-%d-%Y %I:%M%p')
    except Exception:
        try:
            return datetime.datetime.strptime(f"{date_str} {time_str.upper()}", '%m-%d-%Y %I%p')
        except Exception:
            return None

def refresh_eco_calendar():
    global _eco_cache, _eco_cache_date
    today = et_now().date()
    if _eco_cache_date == today:
        return
    events = []
    for week in ('thisweek', 'nextweek'):
        try:
            root = _fetch_ff_calendar(week)
            for ev in root.findall('event'):
                if ev.findtext('country') != 'USD':
                    continue
                if ev.findtext('impact') not in ECO_IMPACT:
                    continue
                dt = _parse_et_time(ev.findtext('date', ''), ev.findtext('time', ''))
                if dt:
                    events.append((dt, ev.findtext('title', '')))
        except Exception as e:
            log(f"WARN  eco-calendar fetch failed ({week}): {e}")
    _eco_cache      = events
    _eco_cache_date = today
    log(f"ECO-CAL  loaded {len(events)} High-impact USD events this/next week")
    for dt, title in sorted(events)[:5]:
        kst = dt + datetime.timedelta(hours=13)  # EDT(UTC-4) → KST(UTC+9)
        log(f"         {dt.strftime('%m-%d %H:%M ET')} ({kst.strftime('%m-%d %H:%M KST')})  {title}")

def in_blackout():
    et = et_now()
    for h, m, dur in BLACKOUT_WINDOWS:
        start = et.replace(hour=h, minute=m, second=0, microsecond=0)
        end   = start + datetime.timedelta(minutes=dur)
        if start <= et < end:
            return True, f"{h:02d}:{m:02d} ET +{dur}min blackout"
    et_naive = et.replace(tzinfo=None)
    for ev_dt, ev_title in _eco_cache:
        delta = abs((et_naive - ev_dt).total_seconds()) / 60
        if delta <= ECO_BLACKOUT_MINS:
            return True, f"eco-event '{ev_title}' at {ev_dt.strftime('%H:%M ET')} (±{ECO_BLACKOUT_MINS}min)"
    return False, None

# ── CLOB client (live mode) ───────────────────────────────────────────────────
clob = None

def init_clob():
    global clob
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds
    sig_type = int(os.getenv('POLYMARKET_SIGNATURE_TYPE', '0'))
    funder = os.getenv('POLYMARKET_PROXY_ADDRESS') or None  # deposit wallet for sig_type=3
    clob = ClobClient(
        host='https://clob.polymarket.com',
        key=os.getenv('POLYMARKET_PRIVATE_KEY'),
        chain_id=137,
        creds=ApiCreds(
            api_key=os.getenv('POLYMARKET_API_KEY'),
            api_secret=os.getenv('POLYMARKET_SECRET'),
            api_passphrase=os.getenv('POLYMARKET_PASSPHRASE'),
        ),
        signature_type=sig_type,
        funder=funder,
    )
    return clob

def place_live_order(token_id, price, size, side):
    from py_clob_client_v2.clob_types import OrderArgsV2, CreateOrderOptions
    BUILDER_CODE = '0x0000000000000000000000000000000000000000000000000000000000000000'
    try:
        order = clob.create_order(
            OrderArgsV2(
                token_id=token_id,
                price=round(price, 4),
                size=round(size, 4),
                side=side,
                builder_code=BUILDER_CODE,
            ),
            options=CreateOrderOptions(tick_size="0.01", neg_risk=False),
        )
        resp = clob.post_order(order)
        return resp
    except Exception as e:
        log(f"ORDER-ERROR  {e}")
        return None

_POLYGON_RPCS = [
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
    "https://polygon.meowrpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
]

def get_live_usdc():
    """Read pUSD balance from the deposit wallet (or EOA if no proxy address)."""
    try:
        from eth_account import Account
        proxy = os.getenv('POLYMARKET_PROXY_ADDRESS')
        if proxy:
            address = proxy  # deposit wallet holds the funds for sig_type=3
        else:
            address = Account.from_key(os.getenv('POLYMARKET_PRIVATE_KEY')).address
        USDC_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        data = "0x70a08231" + address[2:].lower().zfill(64)
        payload = json.dumps({
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": USDC_CONTRACT, "data": data}, "latest"],
            "id": 1,
        }).encode()
        for rpc in _POLYGON_RPCS:
            try:
                req = urllib.request.Request(
                    rpc, data=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp = json.load(urllib.request.urlopen(req, timeout=5))
                if 'result' in resp:
                    return int(resp['result'], 16) / 1e6
            except Exception:
                continue
        return None
    except Exception:
        return None

# ── logging setup (daily rotate, 30일 보관) ────────────────────────────────────
def _gz_rotator(source: str, dest: str):
    """rotate 시 gzip으로 압축 후 원본 삭제."""
    import gzip, shutil
    gz_path = dest + '.gz'
    with open(source, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(source)

def _setup_logger() -> logging.Logger:
    LOG_DIR = Path(os.environ.get('ROOT', Path.home() / 'polymarket-arb')) / 'logs'
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('sniper')
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    # 파일 핸들러: 매일 자정 rotate, 30일 보관, gzip 압축
    fh = TimedRotatingFileHandler(
        LOG_DIR / 'sniper.log',
        when='midnight', interval=1, backupCount=30,
        encoding='utf-8', utc=False,
    )
    fh.suffix = '%Y-%m-%d'
    fh.rotator = _gz_rotator
    fh.namer = lambda name: name + '.gz'
    fh.setFormatter(logging.Formatter('%(message)s'))
    # 콘솔 핸들러: stdout 그대로 출력
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

_logger = _setup_logger()

# ── helpers ───────────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    _logger.info(f"{ts}  {msg}")

def fetch(url, timeout=4):
    """Fetch JSON with hard deadline via a daemon thread to prevent silent hangs."""
    result, error = [None], [None]
    def _do():
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'expiry-sniper/1.0'})
            result[0] = json.load(urllib.request.urlopen(req, timeout=timeout))
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout + 3)
    if t.is_alive():
        raise TimeoutError(f"fetch hung: {url}")
    if error[0]:
        raise error[0]
    return result[0]

def read_markets():
    result = {}
    for f in STATE_DIR.glob('*.market'):
        lines = f.read_text().strip().splitlines()
        if len(lines) < 4:
            continue
        try:
            end = datetime.datetime.fromisoformat(lines[2].replace('Z', '+00:00'))
            result[lines[0]] = {
                'slug': lines[0], 'question': lines[1],
                'end': end, 'port': int(lines[3])
            }
        except Exception:
            continue
    return result

def get_orderbook(port, slug):
    try:
        return fetch(f"http://127.0.0.1:{port}/api/orderbook?slug={slug}")
    except Exception:
        return None

_BINANCE_SYMBOL = {
    'btc':  'BTCUSDT',
    'eth':  'ETHUSDT',
    'sol':  'SOLUSDT',
    'bnb':  'BNBUSDT',
    'xrp':  'XRPUSDT',
    'doge': 'DOGEUSDT',
}
_BYBIT_SYMBOL = {}

def _fetch_klines(coin: str, limit: int) -> list:
    """코인에 맞는 거래소(Binance/Bybit)에서 1분봉 close 가격 리스트 반환 (오래된 순).
    Binance: [openTime, o, h, l, c, v, ...] 오래된 순
    Bybit:   [startTime, o, h, l, c, v, turnover] 최신 순 → 반전 필요"""
    coin = coin.lower()
    if coin in _BYBIT_SYMBOL:
        sym = _BYBIT_SYMBOL[coin]
        url = (f"https://api.bybit.com/v5/market/kline"
               f"?category=linear&symbol={sym}&interval=1&limit={limit}")
        raw = fetch(url, timeout=5)
        candles = raw.get('result', {}).get('list', [])
        if not candles:
            return []
        return [float(c[4]) for c in reversed(candles)]  # Bybit은 최신순이므로 역순
    if coin in _BINANCE_SYMBOL:
        sym = _BINANCE_SYMBOL[coin]
        url = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1m&limit={limit}"
        raw = fetch(url, timeout=4)
        return [float(d[4]) for d in raw]
    return []

def _fetch_ohlc(coin: str, limit: int) -> tuple:
    """(closes, highs, lows) 리스트 튜플 반환. Binance/Bybit 통합."""
    coin = coin.lower()
    if coin in _BYBIT_SYMBOL:
        sym = _BYBIT_SYMBOL[coin]
        url = (f"https://api.bybit.com/v5/market/kline"
               f"?category=linear&symbol={sym}&interval=1&limit={limit}")
        raw = fetch(url, timeout=5)
        candles = list(reversed(raw.get('result', {}).get('list', [])))
        closes = [float(c[4]) for c in candles]
        highs  = [float(c[2]) for c in candles]
        lows   = [float(c[3]) for c in candles]
        return closes, highs, lows
    if coin in _BINANCE_SYMBOL:
        sym = _BINANCE_SYMBOL[coin]
        url = f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1m&limit={limit}"
        raw = fetch(url, timeout=4)
        closes = [float(d[4]) for d in raw]
        highs  = [float(d[2]) for d in raw]
        lows   = [float(d[3]) for d in raw]
        return closes, highs, lows
    return [], [], []

def _linreg_r2(values):
    """선형회귀 slope(% 단위)와 R² 반환.
    slope: 캔들당 가격 변화율(%), R²: 추세 일관성(0~1)."""
    n = len(values)
    if n < 3:
        return 0.0, 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    ss_xy = sum((xs[i] - mean_x) * (values[i] - mean_y) for i in range(n))
    ss_xx = sum((xs[i] - mean_x) ** 2 for i in range(n))
    ss_yy = sum((values[i] - mean_y) ** 2 for i in range(n))
    if ss_xx == 0 or ss_yy == 0:
        return 0.0, 0.0
    slope = ss_xy / ss_xx
    r2 = (ss_xy ** 2) / (ss_xx * ss_yy)
    slope_pct = slope / mean_y * 100  # 캔들당 slope를 % 변화율로 변환
    return slope_pct, r2

def get_momentum(coin):
    """1분봉 선형회귀 기반 모멘텀 반환 (Binance/Bybit 자동 선택).
    Returns (direction, slope_pct, r2) or (None, 0, 0).
    slope_pct: 분당 가격 변화율(%), r2: 추세 일관성(0~1).
    베팅 조건: abs(slope_pct) >= MIN_SLOPE_PCT AND r2 >= MIN_R2."""
    if coin.lower() not in _BINANCE_SYMBOL and coin.lower() not in _BYBIT_SYMBOL:
        return None, 0.0, 0.0
    try:
        limit = max(MOMENTUM_MINS + 2, 6)
        closes = _fetch_klines(coin, limit)
        if len(closes) < 4:
            return None, 0.0, 0.0
        window = closes[-(MOMENTUM_MINS + 1):]
        slope_pct, r2 = _linreg_r2(window)
        direction = 'Up' if slope_pct > 0 else 'Down'
        return direction, slope_pct, r2
    except Exception:
        return None, 0.0, 0.0

# ── ML Feature Collection ────────────────────────────────────────────────────

def _wma(prices, period):
    """Weighted Moving Average."""
    w = list(range(1, period + 1))
    return sum(p * wt for p, wt in zip(prices[-period:], w)) / sum(w)

def _hma(prices, period):
    """Hull Moving Average(period). prices 리스트 길이 >= period 필요."""
    if len(prices) < period:
        return None
    half = max(period // 2, 1)
    sqrt_p = max(int(math.sqrt(period)), 1)
    wma_half = [_wma(prices[:i+1], min(half, i+1)) for i in range(len(prices))]
    wma_full = [_wma(prices[:i+1], min(period, i+1)) for i in range(len(prices))]
    diff = [2 * h - f for h, f in zip(wma_half, wma_full)]
    if len(diff) < sqrt_p:
        return None
    return _wma(diff, sqrt_p)

_feature_cache: dict = {}  # coin → features dict, cleared each cycle

def get_binance_features(coin: str) -> dict:
    """1분봉 60개로 HMA55, 변동성, 현재가 등 ML 피처 반환. Binance/Bybit 자동 선택. 사이클당 캐시."""
    if coin in _feature_cache:
        return _feature_cache[coin]
    if coin.lower() not in _BINANCE_SYMBOL and coin.lower() not in _BYBIT_SYMBOL:
        return {}
    try:
        closes, highs, lows = _fetch_ohlc(coin, 60)
        if len(closes) < 10:
            return {}
        price_now = closes[-1]
        hma55 = _hma(closes, 55)
        price_vs_hma = round((price_now - hma55) / hma55 * 100, 4) if hma55 else None
        vol5 = round((max(highs[-5:]) - min(lows[-5:])) / min(lows[-5:]) * 100, 4)
        result = {
            'price_now':        round(price_now, 4),
            'hma55':            round(hma55, 4) if hma55 else None,
            'price_vs_hma_pct': price_vs_hma,
            'volatility_5m_pct': vol5,
        }
        _feature_cache[coin] = result
        return result
    except Exception:
        return {}

def _et_now():
    """현재 ET 시각 (EDT=UTC-4, EST=UTC-5). 5월~11월은 EDT."""
    utc = datetime.datetime.now(datetime.timezone.utc)
    et  = utc - datetime.timedelta(hours=4)  # EDT
    return et

def ml_db_init():
    """ML DB 테이블 생성 및 마이그레이션."""
    con = sqlite3.connect(ML_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                 TEXT    NOT NULL,
            coin               TEXT    NOT NULL,
            interval           TEXT    NOT NULL,
            decision           TEXT    NOT NULL,
            skip_reason        TEXT,
            secs_left          REAL,
            ask                REAL,
            momentum_pct       REAL,
            direction          TEXT,
            candle_consistency INTEGER,
            price_now          REAL,
            hma55              REAL,
            price_vs_hma_pct   REAL,
            volatility_5m_pct  REAL,
            hour_et            INTEGER,
            minute_et          INTEGER,
            weekday            INTEGER,
            trigger_used       INTEGER,
            min_ask_used       REAL,
            result             TEXT,
            net_pnl            REAL
        )
    """)
    # 기존 DB에 candle_consistency 컬럼 없으면 추가
    try:
        con.execute('ALTER TABLE decisions ADD COLUMN candle_consistency INTEGER')
    except sqlite3.OperationalError:
        pass
    con.commit()
    con.close()

def ml_save_decision(coin, interval, decision, skip_reason,
                     secs_left, ask, momentum_pct, direction,
                     binance_feat, trigger_used, min_ask_used,
                     candle_consistency=None) -> int:
    """결정 레코드 저장. 저장된 row id 반환."""
    et   = _et_now()
    ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        con = sqlite3.connect(ML_DB)
        cur = con.execute("""
            INSERT INTO decisions
              (ts, coin, interval, decision, skip_reason,
               secs_left, ask, momentum_pct, direction, candle_consistency,
               price_now, hma55, price_vs_hma_pct, volatility_5m_pct,
               hour_et, minute_et, weekday,
               trigger_used, min_ask_used)
            VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?, ?,?)
        """, (
            ts, coin, interval, decision, skip_reason,
            round(secs_left, 1), ask, round(momentum_pct, 4), direction, candle_consistency,
            binance_feat.get('price_now'), binance_feat.get('hma55'),
            binance_feat.get('price_vs_hma_pct'), binance_feat.get('volatility_5m_pct'),
            et.hour, et.minute, et.weekday(),
            trigger_used, min_ask_used,
        ))
        row_id = cur.lastrowid
        con.commit()
        con.close()
        return row_id
    except Exception:
        return -1

def ml_update_result(row_id: int, result: str, net_pnl: float):
    """결과 확정 시 해당 결정 레코드 업데이트."""
    if row_id < 0:
        return
    try:
        con = sqlite3.connect(ML_DB)
        con.execute("UPDATE decisions SET result=?, net_pnl=? WHERE id=?",
                    (result, round(net_pnl, 4), row_id))
        con.commit()
        con.close()
    except Exception:
        pass

ml_db_init()

# ─────────────────────────────────────────────────────────────────────────────

def pick_by_direction(ob, direction):
    """모멘텀 방향과 일치하는 outcome 토큰 반환."""
    for o in ob.get('outcomes', []):
        if o.get('outcome', '').lower() == direction.lower():
            return o
    return None

def check_resolution(condition_id, slug):
    """Return the winning outcome string, or None if not yet resolved."""
    # Primary: CLOB API via condition_id (reliable, returns tokens[].winner)
    if condition_id and condition_id.startswith('0x'):
        try:
            raw = fetch(f"https://clob.polymarket.com/markets/{condition_id}", timeout=5)
            if raw and raw.get('closed'):
                for t in raw.get('tokens', []):
                    if t.get('winner'):
                        return t.get('outcome')
        except Exception:
            pass
    # Fallback: gamma-api by slug — only use if market is actually closed
    try:
        raw = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1", timeout=5)
        m = raw[0] if raw else None
        if m and m.get('closed'):
            prices   = json.loads(m.get('outcomePrices') or '[]')
            outcomes = json.loads(m.get('outcomes') or '[]')
            for i, p in enumerate(prices):
                if float(p) >= 0.95 and i < len(outcomes):
                    return outcomes[i]
    except Exception:
        pass
    return None

def save_result(bet):
    write_header = not RESULTS_CSV.exists() or RESULTS_CSV.stat().st_size == 0
    with open(RESULTS_CSV, 'a', newline='') as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(['time', 'slug', 'question', 'bet_outcome',
                        'ask', 'implied_pct', 'fee', 'winner', 'won', 'net_pnl', 'mode'])
        w.writerow([
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            bet['slug'], bet['question'][:60], bet['outcome'],
            f"{bet['ask']:.4f}", f"{bet['bid']*100:.0f}",
            f"{bet['fee']:.4f}",
            bet.get('winner', '?'), bet.get('won', '?'),
            f"{bet.get('pnl', 0):.4f}",
            bet.get('mode', 'paper'),
        ])

def print_stats(wins, losses, pnl):
    total    = wins + losses
    win_rate = wins / total * 100 if total else 0
    if PAPER_CAPITAL > 0:
        balance = PAPER_CAPITAL + pnl
        log(f"── Stats: {wins}W / {losses}L  win-rate={win_rate:.0f}%  PnL=${pnl:+.2f}  잔고=${balance:.2f} ──")
    else:
        log(f"── Stats: {wins}W / {losses}L  win-rate={win_rate:.0f}%  PnL=${pnl:+.2f} ──")

def save_pending_bets(bets_dict):
    """미결 베팅을 디스크에 저장 (재시작 후 복구용)."""
    pending = {}
    for slug, bet in bets_dict.items():
        if bet.get('resolved'):
            continue
        b = dict(bet)
        if isinstance(b.get('end'), datetime.datetime):
            b['end'] = b['end'].isoformat()
        pending[slug] = b
    try:
        PENDING_BETS_FILE.write_text(json.dumps(pending, indent=2))
    except Exception as e:
        log(f"WARN  pending bets 저장 실패: {e}")

def load_pending_bets():
    """이전 세션의 미결 베팅을 복구. GIVE_UP 초과 건은 제외."""
    if not PENDING_BETS_FILE.exists():
        return {}
    try:
        data = json.loads(PENDING_BETS_FILE.read_text())
        now = datetime.datetime.now(datetime.timezone.utc)
        recovered = {}
        for slug, bet in data.items():
            if isinstance(bet.get('end'), str):
                bet['end'] = datetime.datetime.fromisoformat(bet['end'])
            secs_since = (now - bet['end']).total_seconds()
            if secs_since < GIVE_UP:
                recovered[slug] = bet
        if recovered:
            log(f"RESTORE  이전 세션 미결 베팅 {len(recovered)}건 복구: "
                f"{', '.join(recovered)}")
        return recovered
    except Exception as e:
        log(f"WARN  pending bets 로드 실패: {e}")
        return {}

# ── Telegram command listener ────────────────────────────────────────────────
def _daily_pnl_kst() -> float:
    """오늘 KST 기준 PnL 합산 (현재 MODE). Phase 1 daily loss cap 체크용."""
    total = 0.0
    for row in _csv_today_rows():
        try:
            total += float(row.get('net_pnl', 0) or 0)
        except (ValueError, TypeError):
            pass
    return total


def _csv_today_rows():
    """오늘 KST 기준 현재 MODE의 CSV 행 반환."""
    if not RESULTS_CSV.exists():
        return []
    kst_today = (datetime.datetime.now(datetime.timezone.utc)
                 + datetime.timedelta(hours=9)).date().isoformat()
    rows = []
    try:
        with open(RESULTS_CSV, newline='') as f:
            for row in csv.DictReader(f):
                if not row.get('time', '').startswith(kst_today):
                    continue
                mode_col = row.get('', '').strip().strip("[]'\"")
                if MODE not in mode_col:
                    continue
                rows.append(row)
    except Exception:
        pass
    return rows

def _slug_to_coin_iv(slug):
    """slug에서 코인명과 interval 추출."""
    coin = next((c for c in ALLOWED_COINS if c in slug), slug[:3])
    iv = '15m' if '15m' in slug else ('5m' if '5m' in slug else '?m')
    return coin.upper(), iv

def _tg_config_text() -> str:
    """코인별 + 전역 설정값 요약 텍스트."""
    lines = ["⚙️ <b>현재 설정값</b>\n"]
    lines.append(f"{'전역':>4}  bet=${BET_SIZE:.0f}  min_ask={MIN_ASK:.2f}  "
                 f"slope≥{MIN_SLOPE_PCT:.3f}%  R²≥{MIN_R2:.2f}  "
                 f"5m={os.environ.get('TRIGGER_5M', str(TRIGGER))}s  "
                 f"15m={os.environ.get('TRIGGER_15M', str(TRIGGER))}s")
    lines.append("")
    for coin in ALLOWED_COINS:
        c = coin.upper()
        bet   = coin_cfg(coin, 'BET_SIZE', BET_SIZE)
        mna   = coin_cfg(coin, 'MIN_ASK', MIN_ASK)
        t5    = get_trigger(coin, '5m')
        t15   = get_trigger(coin, '15m')
        ivs   = sorted(_coin_interval_filter.get(coin, set()))
        iv_s  = ','.join(ivs) if ivs else '전체'
        lines.append(f"<b>{c:>4}</b>  bet=${bet:.0f}  min_ask={mna:.2f}  "
                     f"5m={t5}s  15m={t15}s  iv={iv_s}")
    lines.append(f"\n연속손실경고: {CONSEC_LOSS_ALERT}연패  stop_loss={STOP_LOSS_PCT*100:.0f}%")
    return '\n'.join(lines)

def _tg_daily_text():
    rows = _csv_today_rows()
    kst_today = (datetime.datetime.now(datetime.timezone.utc)
                 + datetime.timedelta(hours=9)).date().isoformat()
    if not rows:
        return f"{kst_today} 거래 없음"
    wins = sum(1 for r in rows if r.get('won') == 'True')
    losses = len(rows) - wins
    pnl = sum(float(r.get('net_pnl', 0)) for r in rows)
    rate = f"{wins/(wins+losses)*100:.0f}%" if (wins+losses) else "-"
    lines = [f"📊 <b>{kst_today} 매매내역</b>  {wins}승 {losses}패  {rate}\n"]
    for r in rows:
        t = r.get('time', '')[11:16]
        coin, iv = _slug_to_coin_iv(r.get('slug', ''))
        out = r.get('bet_outcome', '?')
        w   = '✅' if r.get('won') == 'True' else '❌'
        p   = float(r.get('net_pnl', 0))
        lines.append(f"  {t}  {coin}/{iv}  {out}  {w}  ${p:+.2f}")
    lines.append(f"\n💰 총손익: <b>${pnl:+.2f}</b>")
    return '\n'.join(lines)

def _csv_total_pnl():
    """paper 모드 거래만 누적 손익 합산."""
    if not RESULTS_CSV.exists():
        return 0.0
    total = 0.0
    try:
        with open(RESULTS_CSV, newline='') as f:
            for row in csv.DictReader(f):
                mode_col = row.get('', '').strip().strip("[]'\"")
                if 'paper' in mode_col:
                    total += float(row.get('net_pnl', 0))
    except Exception:
        pass
    return total

def _tg_status_text():
    if MODE == 'live':
        usdc = get_live_usdc()
        bal_str = f"${usdc:.2f}" if usdc is not None else "확인불가"
    else:
        cum_pnl = _csv_total_pnl()
        balance = PAPER_CAPITAL + cum_pnl if PAPER_CAPITAL > 0 else None
        bal_str = f"${balance:.2f} (paper)" if balance is not None else "확인불가"
    rows = _csv_today_rows()
    if not rows:
        return f"💼 <b>현재 상태</b>\n잔고: <b>{bal_str}</b>\n오늘 거래 없음"
    wins = sum(1 for r in rows if r.get('won') == 'True')
    losses = len(rows) - wins
    pnl = sum(float(r.get('net_pnl', 0)) for r in rows)
    total = wins + losses
    rate = f"{wins/total*100:.0f}%" if total else "-"
    return (f"💼 <b>현재 상태</b>\n"
            f"잔고: <b>{bal_str}</b>\n"
            f"오늘: {wins}승 {losses}패  승률 {rate}  손익 ${pnl:+.2f}")

_tg_update_offset = 0

def _tg_init_offset():
    """시작 시 이미 쌓인 메시지를 건너뛰기 위해 최신 offset으로 초기화"""
    global _tg_update_offset
    if not _TG_TOKEN:
        return
    try:
        url = f'https://api.telegram.org/bot{_TG_TOKEN}/getUpdates?offset=-1&timeout=0'
        req = urllib.request.Request(url, headers={'User-Agent': 'expiry-sniper/1.0'})
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
        results = data.get('result', [])
        if results:
            _tg_update_offset = results[-1]['update_id'] + 1
    except Exception:
        pass

def _tg_poll_loop():
    global _tg_update_offset
    if not _TG_TOKEN or not _TG_CHAT:
        return
    _tg_init_offset()
    log("TG-CMD  텔레그램 명령어 수신 대기 중 (/status /daily /help)")
    while True:
        try:
            url = (f'https://api.telegram.org/bot{_TG_TOKEN}/getUpdates'
                   f'?offset={_tg_update_offset}&timeout=30')
            req = urllib.request.Request(url, headers={'User-Agent': 'expiry-sniper/1.0'})
            data = json.loads(urllib.request.urlopen(req, timeout=35).read())
            for upd in data.get('result', []):
                _tg_update_offset = upd['update_id'] + 1
                msg = upd.get('message', {})
                chat_id = str(msg.get('chat', {}).get('id', ''))
                raw_text = msg.get('text', '').strip() if msg.get('text') else ''
                parts = raw_text.lower().split()
                cmd = parts[0] if parts else ''
                if chat_id != str(_TG_CHAT):
                    continue
                if cmd in ('/daily', '/오늘'):
                    send_telegram(_tg_daily_text())
                elif cmd in ('/status', '/상태'):
                    send_telegram(_tg_status_text())
                elif cmd in ('/config', '/설정'):
                    send_telegram(_tg_config_text())
                elif cmd == '/add' and len(parts) >= 3:
                    coin_arg, iv_arg = parts[1], parts[2]
                    if coin_arg not in ALLOWED_COINS:
                        send_telegram(f"❌ 알 수 없는 코인: {coin_arg.upper()}\n허용: {', '.join(ALLOWED_COINS)}")
                    elif iv_arg not in ('5m', '15m'):
                        send_telegram(f"❌ 알 수 없는 interval: {iv_arg}\n허용: 5m, 15m")
                    else:
                        _coin_interval_filter[coin_arg].add(iv_arg)
                        ivs = sorted(_coin_interval_filter[coin_arg])
                        _save_env_key(f"{coin_arg.upper()}_INTERVALS", ','.join(ivs))
                        log(f"TG-CMD  /add {coin_arg.upper()} {iv_arg} → 허용: {ivs or '전체'} (.env 저장)")
                        send_telegram(f"✅ {coin_arg.upper()} 허용 interval: <b>{', '.join(ivs) or '전체'}</b>")
                elif cmd == '/del' and len(parts) >= 3:
                    coin_arg, iv_arg = parts[1], parts[2]
                    if coin_arg not in ALLOWED_COINS:
                        send_telegram(f"❌ 알 수 없는 코인: {coin_arg.upper()}\n허용: {', '.join(ALLOWED_COINS)}")
                    elif iv_arg not in ('5m', '15m'):
                        send_telegram(f"❌ 알 수 없는 interval: {iv_arg}\n허용: 5m, 15m")
                    else:
                        _coin_interval_filter[coin_arg].discard(iv_arg)
                        ivs = sorted(_coin_interval_filter[coin_arg])
                        _save_env_key(f"{coin_arg.upper()}_INTERVALS", ','.join(ivs))
                        log(f"TG-CMD  /del {coin_arg.upper()} {iv_arg} → 허용: {ivs or '전체'} (.env 저장)")
                        send_telegram(f"✅ {coin_arg.upper()} 허용 interval: <b>{', '.join(ivs) or '전체'}</b>")
                elif cmd == '/set' and len(parts) >= 3:
                    send_telegram(tg_set_value(parts[1], parts[2]))
                elif cmd == '/set':
                    keys = '\n'.join(f"  {k}" for k in _SETTABLE)
                    send_telegram(f"사용법: /set &lt;KEY&gt; &lt;값&gt;\n\n설정 가능한 키:\n{keys}")
                elif cmd == '/restart':
                    send_telegram("🔄 Expiry Sniper 재시작 중...")
                    log("TG-CMD  /restart 수신 — 재시작")
                    sh = Path(__file__).parent / 'expiry-sniper.sh'
                    subprocess.Popen(['bash', str(sh), 'restart'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                elif cmd == '/stop':
                    send_telegram("⛔ Expiry Sniper 중지합니다.")
                    log("TG-CMD  /stop 수신 — 중지")
                    sh = Path(__file__).parent / 'expiry-sniper.sh'
                    subprocess.Popen(['bash', str(sh), 'stop'],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                elif cmd in ('/help', '/도움'):
                    send_telegram(
                        "📋 <b>사용 가능한 명령어</b>\n"
                        "/status        — 현재 잔고 + 오늘 실적\n"
                        "/daily         — 오늘 전체 매매내역\n"
                        "/config        — 코인별 설정값 확인\n"
                        "/add &lt;코인&gt; &lt;5m|15m&gt; — interval 추가\n"
                        "/del &lt;코인&gt; &lt;5m|15m&gt; — interval 제거\n"
                        "/set &lt;KEY&gt; &lt;값&gt; — 설정값 변경 (즉시 + .env 저장)\n"
                        "/restart       — 스나이퍼 재시작\n"
                        "/stop          — 스나이퍼 중지\n"
                        "/help          — 이 도움말"
                    )
        except Exception:
            time.sleep(5)

threading.Thread(target=_tg_poll_loop, daemon=True).start()

# ── main ─────────────────────────────────────────────────────────────────────
_bw_str = ','.join(f"{h:02d}:{m:02d}+{d}m" for h, m, d in BLACKOUT_WINDOWS)
log(f"Expiry Sniper started  mode={MODE}  bet=${BET_SIZE}  fee={FEE_RATE*100:.2f}%  "
    f"min_net=${MIN_NET_PNL}  min_ask={MIN_ASK}  stop_loss={STOP_LOSS_PCT*100:.0f}%  "
    f"trigger=5m:{os.environ.get('TRIGGER_5M') or os.environ.get('5M_TRIGGER', str(TRIGGER))}s"
    f"/15m:{os.environ.get('TRIGGER_15M') or os.environ.get('15M_TRIGGER', str(TRIGGER))}s  "
    f"min_entry={MIN_ENTRY_SECS}s  coins={','.join(ALLOWED_COINS)}  blackout_ET=[{_bw_str}]")

if MODE == 'live':
    init_clob()
    usdc = get_live_usdc()
    log(f"LIVE MODE  USDC balance=${usdc:.2f}" if usdc is not None else "LIVE MODE  (balance check failed)")
    if usdc is not None and usdc < BET_SIZE:
        log(f"WARN  USDC balance ${usdc:.2f} < BET_SIZE ${BET_SIZE:.2f} — insufficient funds")

bets              = load_pending_bets()
wins              = 0
losses            = 0
pnl               = 0.0
warn_at           = {}
consecutive_losses = 0
_consec_cooldown_until: float = 0.0          # 방법1: 전체 쿨다운 만료 시각 (epoch)
_coin_last_loss: dict = {}                    # 방법2: {coin: 손실 발생 epoch}

_blackout_warned = False
_daily_cap_warned: str = ''  # KST date string when warning was last sent (auto-reset per day)
_ask_skip_last: dict = {}  # slug -> last logged ask value (avoid repeated ask-range log spam)

while True:
    refresh_eco_calendar()
    now     = datetime.datetime.now(datetime.timezone.utc)
    markets = read_markets()

    # ── 1. Place new bets ────────────────────────────────────────────────────
    blackout, blackout_reason = in_blackout()
    if blackout:
        if not _blackout_warned:
            log(f"BLACKOUT  betting paused — {blackout_reason}")
            _blackout_warned = True
        time.sleep(INTERVAL)
        continue
    _blackout_warned = False

    # ── Phase 1: 일일 손실 cap (KST 기준) ─────────────────────────────────
    # 5/18 같은 -$94 day 방어. 자정 KST 자동 리셋. CSV 일별 합산 (현 MODE 만).
    if DAILY_LOSS_CAP_USD > 0:
        _daily_pnl = _daily_pnl_kst()
        if _daily_pnl <= -DAILY_LOSS_CAP_USD:
            _kst_today_iso = (datetime.datetime.now(datetime.timezone.utc)
                              + datetime.timedelta(hours=9)).date().isoformat()
            if _daily_cap_warned != _kst_today_iso:
                log(f"DAILY-CAP  loss=${_daily_pnl:.2f} ≤ -${DAILY_LOSS_CAP_USD:.2f} — 신규 진입 차단 (자정 KST 리셋)")
                send_telegram(
                    f"🚨 <b>일일 손실 cap 도달</b>\n"
                    f"오늘 PnL: ${_daily_pnl:.2f}\n"
                    f"Cap: -${DAILY_LOSS_CAP_USD:.2f}\n"
                    f"신규 진입 차단 — 자정 KST 자동 리셋"
                )
                _daily_cap_warned = _kst_today_iso
            time.sleep(INTERVAL)
            continue

    # 알트 코인 모멘텀 사전 스캔 (사이클당 1회 — 중복 API 호출 방지)
    _alt_dir_cache = {}
    for _c in [c for c in ALLOWED_COINS if c in _ALT_GROUP]:
        _alt_dir_cache[_c] = get_momentum(_c)  # (direction, slope_pct, r2)

    # ML 피처 캐시 초기화 (사이클당 1회)
    _feature_cache.clear()

    def _slug_duration(s):
        return 15 if '15m' in s else (5 if '5m' in s else 99)

    def _slug_interval(s):
        return '15m' if '15m' in s else ('5m' if '5m' in s else '?m')

    def _slug_coin(s):
        return next((c for c in ALLOWED_COINS if c in s), None)

    # 정렬: duration 오름차순, 동일 duration 내에서는 코인 우선순위(ETH→BTC→BNB→SOL) 순
    sorted_markets = sorted(markets.items(), key=lambda kv: (
        _slug_duration(kv[0]),
        _COIN_ENTER_PRIORITY.get(_slug_coin(kv[0]) or '', 99),
    ))

    for slug, m in sorted_markets:
        secs_left = (m['end'] - now).total_seconds()
        coin = _slug_coin(slug)
        if not coin:
            continue
        interval = _slug_interval(slug)
        trigger = get_trigger(coin, interval)
        if slug in bets or not (MIN_ENTRY_SECS < secs_left <= trigger):
            continue

        # 코인별 허용 interval 필터 (예: SOL은 15m만)
        allowed_ivs = coin_intervals(coin)
        if allowed_ivs and interval not in allowed_ivs:
            bets[slug] = {'resolved': True, 'skipped': True}
            continue

        # 같은 코인+인터벌에 이미 활성 베팅(미결)이 있으면 건너뜀 (5m/15m는 독립)
        if any(_slug_coin(s) == coin and _slug_interval(s) == interval and not b.get('resolved')
               for s, b in bets.items()):
            log(f"SKIP [{interval}]  {m['question'][:55]}\n"
                f"      {coin.upper()} [{interval}] 이미 활성 베팅 있음 — 중복 건너뜀")
            bets[slug] = {'resolved': True, 'skipped': True}
            continue

        # ── Phase 1: 동시 진입 limit (5/18 09:23 4-trade 동시 catastrophic 방어) ──
        # 일시 차단 — bets[slug] 영구 mark 안 함 (다음 cycle 활성 bet 종료 후 재시도 가능)
        if MAX_CONCURRENT_BETS > 0:
            _active_bets = sum(1 for b in bets.values() if not b.get('resolved'))
            if _active_bets >= MAX_CONCURRENT_BETS:
                log(f"SKIP [{interval}]  {coin.upper()} — 동시 진입 limit ({_active_bets}/{MAX_CONCURRENT_BETS})")
                continue

        # ── 모멘텀 확인 (선형회귀 slope + R²) ───────────────────────
        c_min_slope = coin_cfg(coin, 'MIN_SLOPE_PCT', MIN_SLOPE_PCT)
        c_min_r2    = coin_cfg(coin, 'MIN_R2', MIN_R2)
        direction, pct, r2 = _alt_dir_cache[coin] if coin in _alt_dir_cache else get_momentum(coin)
        consistency = round(r2 * 10)  # DB 저장용 호환 (0~10)

        def _skip_all_same_coin(reason_msg):
            for s2, m2 in sorted_markets:
                secs2 = (m2['end'] - now).total_seconds()
                iv2 = _slug_interval(s2)
                if _slug_coin(s2) == coin and s2 not in bets and MIN_ENTRY_SECS < secs2 <= get_trigger(coin, iv2):
                    if s2 != slug:
                        log(f"SKIP [{_slug_interval(s2)}]  {m2['question'][:55]}\n      {reason_msg}")
                    bets[s2] = {'resolved': True, 'skipped': True}

        if direction is None or abs(pct) < c_min_slope:
            reason = f"slope 부족 — {coin.upper()} slope={pct:+.4f}%/분 (최소 {c_min_slope:.4f}%)"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            ml_save_decision(coin, interval, 'SKIP', 'momentum',
                             secs_left, 0.0, pct, direction or '',
                             get_binance_features(coin), trigger, coin_cfg(coin, 'MIN_ASK', MIN_ASK),
                             candle_consistency=round(r2 * 10))
            _skip_all_same_coin(reason)
            continue

        if r2 < c_min_r2:
            reason = f"R² 낮음 — {coin.upper()} R²={r2:.3f} (최소 {c_min_r2:.2f}) slope={pct:+.4f}%"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            ml_save_decision(coin, interval, 'SKIP', 'r2_filter',
                             secs_left, 0.0, pct, direction or '',
                             get_binance_features(coin), trigger, coin_cfg(coin, 'MIN_ASK', MIN_ASK),
                             candle_consistency=round(r2 * 10))
            _skip_all_same_coin(reason)
            continue

        # ── 방법1: 연속 손실 전체 쿨다운 ────────────────────────────────
        if _consec_cooldown_until > time.time():
            remain = int(_consec_cooldown_until - time.time())
            reason = f"연속손실 쿨다운 — {remain}초 남음 ({CONSEC_LOSS_COOLDOWN}연패 후 {CONSEC_LOSS_COOLDOWN_SECS}s 대기)"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            _skip_all_same_coin(reason)
            continue

        # ── Phase 1: 동방향 cluster 차단 (correlated coins same direction) ──
        # 5/18 09:23 BTC/ETH/SOL/XRP 동시 Up 진입 → 모두 -$10 catastrophic 방어.
        # direction 알려진 시점에 check (momentum filter 통과 후).
        if CORRELATED_SAME_DIR_LIMIT > 0 and direction:
            _same_dir = sum(1 for b in bets.values()
                            if not b.get('resolved') and b.get('outcome','').lower() == direction.lower())
            if _same_dir >= CORRELATED_SAME_DIR_LIMIT:
                log(f"SKIP [{interval}]  {coin.upper()} — 동방향({direction}) 진입 limit ({_same_dir}/{CORRELATED_SAME_DIR_LIMIT})")
                continue

        # ── 방법2: 코인별 손실 후 쿨다운 ────────────────────────────────
        coin_loss_ago = time.time() - _coin_last_loss.get(coin, 0)
        if coin_loss_ago < COIN_LOSS_COOLDOWN_SECS:
            remain = int(COIN_LOSS_COOLDOWN_SECS - coin_loss_ago)
            reason = f"코인 쿨다운 — {coin.upper()} 손실 후 {remain}초 남음 (대기 {COIN_LOSS_COOLDOWN_SECS}s)"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            ml_save_decision(coin, interval, 'SKIP', 'coin_cooldown',
                             secs_left, 0.0, pct, direction,
                             get_binance_features(coin), trigger, coin_cfg(coin, 'MIN_ASK', MIN_ASK),
                             candle_consistency=consistency)
            _skip_all_same_coin(reason)
            continue

        # ── 방법4: 변동성 필터 ───────────────────────────────────────────
        _feat = get_binance_features(coin)
        _vol  = _feat.get('volatility_5m_pct', 0.0) or 0.0
        if MAX_VOLATILITY_PCT > 0 and _vol > MAX_VOLATILITY_PCT:
            reason = f"고변동성 — {coin.upper()} vol={_vol:.3f}% > {MAX_VOLATILITY_PCT:.2f}%"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            ml_save_decision(coin, interval, 'SKIP', 'volatility',
                             secs_left, 0.0, pct, direction,
                             _feat, trigger, coin_cfg(coin, 'MIN_ASK', MIN_ASK),
                             candle_consistency=consistency)
            _skip_all_same_coin(reason)
            continue

        # ── 코인별 허용 방향 필터 ─────────────────────────────────────
        c_allowed_dir = os.environ.get(f"{coin.upper()}_ALLOWED_DIRECTION", '').strip().lower()
        if c_allowed_dir and direction.lower() != c_allowed_dir:
            reason = f"방향 제한 — {coin.upper()} {direction} 베팅 불허 (허용: {c_allowed_dir})"
            log(f"SKIP [{interval}]  {m['question'][:55]}\n      {reason}")
            ml_save_decision(coin, interval, 'SKIP', 'direction_filter',
                             secs_left, 0.0, pct, direction,
                             get_binance_features(coin), trigger, coin_cfg(coin, 'MIN_ASK', MIN_ASK),
                             candle_consistency=consistency)
            _skip_all_same_coin(reason)
            continue

        ob = get_orderbook(m['port'], slug)
        if not ob:
            continue
        favored = pick_by_direction(ob, direction)
        if not favored:
            continue

        market_id    = ob.get('market_id', '')
        token_id     = favored.get('token_id', '')
        ask          = favored['best_ask_price']
        bid          = favored['best_bid_price']

        # Fetch condition_id now (market still active) for reliable post-expiry resolution
        condition_id = None
        try:
            gm = fetch(f"https://gamma-api.polymarket.com/markets?slug={slug}&limit=1", timeout=4)
            if gm:
                condition_id = gm[0].get('conditionId')
        except Exception:
            pass

        c_min_ask = coin_cfg(coin, 'MIN_ASK', MIN_ASK)
        c_max_ask = coin_cfg(coin, 'MAX_ASK', MAX_ASK)

        if ask < c_min_ask:
            if _ask_skip_last.get(slug) != ask:
                log(f"SKIP [{interval}]  {m['question'][:55]}\n"
                    f"      outcome={favored['outcome']}  ask={ask:.2f} < min_ask {c_min_ask:.2f} ({coin.upper()})")
                ml_save_decision(coin, interval, 'SKIP', 'ask_low',
                                 secs_left, ask, pct, direction,
                                 get_binance_features(coin), trigger, c_min_ask)
                _ask_skip_last[slug] = ask
            continue  # 재시도 가능 — ask는 시간이 지나면 변함

        if ask > c_max_ask:
            if _ask_skip_last.get(slug) != ask:
                log(f"SKIP [{interval}]  {m['question'][:55]}\n"
                    f"      outcome={favored['outcome']}  ask={ask:.2f} > max_ask {c_max_ask:.2f} ({coin.upper()})")
                ml_save_decision(coin, interval, 'SKIP', 'ask_high',
                                 secs_left, ask, pct, direction,
                                 get_binance_features(coin), trigger, c_min_ask)
                _ask_skip_last[slug] = ask
            continue  # 재시도 가능 — ask는 시간이 지나면 변함

        # 인터벌별 베팅금액 우선: {COIN}_{5M|15M}_BET_SIZE > {COIN}_BET_SIZE > BET_SIZE
        iv_key = interval.upper().replace('M', 'M')
        bet_size = float(os.environ.get(f"{coin.upper()}_{iv_key}_BET_SIZE",
                         coin_cfg(coin, 'BET_SIZE', BET_SIZE)))
        tokens    = bet_size / ask
        gross_win = tokens - bet_size
        fee       = bet_size * FEE_RATE
        net_win   = gross_win - fee

        if net_win < MIN_NET_PNL:
            log(f"SKIP [{interval}]  {m['question'][:55]}\n"
                f"      outcome={favored['outcome']}  ask={ask:.2f}  "
                f"net=${net_win:.2f} < min ${MIN_NET_PNL:.2f}")
            bets[slug] = {'resolved': True, 'skipped': True}
            continue
        c_bet_size = bet_size

        # Place order
        order_id = None
        if MODE == 'live':
            from py_clob_client_v2.order_builder.constants import BUY
            resp = place_live_order(token_id, ask, tokens, BUY)
            if resp is None:
                log(f"ORDER-FAIL  {m['question'][:55]} — skipping")
                bets[slug] = {'resolved': True, 'skipped': True}
                continue
            order_id = resp.get('orderID', '') if isinstance(resp, dict) else str(resp)
            log(f"ORDER-PLACED  id={order_id}")

        _ml_row_id = ml_save_decision(coin, interval, 'BET', None,
                                       secs_left, ask, pct, direction,
                                       get_binance_features(coin), trigger, c_min_ask,
                                       candle_consistency=consistency)

        bets[slug] = {
            'slug': slug, 'market_id': market_id, 'condition_id': condition_id,
            'token_id': token_id,
            'question': m['question'], 'outcome': favored['outcome'],
            'ask': ask, 'bid': bid, 'tokens': tokens,
            'bet': c_bet_size, 'fee': fee, 'profit_if_win': net_win,
            'end': m['end'], 'port': m['port'], 'resolved': False,
            'order_id': order_id, 'stop_loss_triggered': False,
            'mode': MODE, 'ml_row_id': _ml_row_id,
            'peak_bid': bid,  # 익절 트레일링용 최고 bid 추적
        }

        max_loss_est = round(c_bet_size * (1 - STOP_LOSS_PCT), 2)
        log(f"BET   {m['question'][:55]}\n"
            f"      outcome={favored['outcome']}  ask={ask:.2f}  "
            f"slope={pct:+.4f}%/분  R²={r2:.3f}  secs_left={secs_left:.0f}  id={market_id}\n"
            f"      gross=+${gross_win:.2f}  fee=-${fee:.2f}  "
            f"net=+${net_win:.2f}  lose→-${c_bet_size:.2f}  stop-loss→-${max_loss_est:.2f}")
        send_telegram(
            f"🎯 <b>BET [{interval}]</b> {coin.upper()}\n"
            f"방향: {favored['outcome']}  ask={ask:.2f}  R²={r2:.3f}\n"
            f"베팅: ${c_bet_size:.2f}  승시: +${net_win:.2f}  최대손실: -${max_loss_est:.2f}\n"
            f"모멘텀: {pct:+.3f}%/분  남은시간: {secs_left:.0f}s"
        )
        save_pending_bets(bets)

    # ── 2. Stop-loss monitor (live mode, pre-expiry) ─────────────────────────
    # Stop-loss: 5m 마켓은 orderbook이 너무 얇아 오발동 위험 → 15m 이상만 적용.
    # 마감 STOP_LOSS_FLOOR_SECS(60s) 이내는 유동성 부족으로 손절 불가 — 결과 기다림.
    if MODE == 'live':
        for slug, bet in list(bets.items()):
            if bet.get('resolved') or bet.get('stop_loss_triggered'):
                continue
            secs_left = (bet['end'] - now).total_seconds()
            if secs_left <= 0 or secs_left <= STOP_LOSS_FLOOR_SECS:
                continue  # expired or final seconds: wait for result

            ob = get_orderbook(bet['port'], slug)
            if not ob:
                continue
            for outcome in ob.get('outcomes', []):
                if outcome.get('token_id') != bet['token_id']:
                    continue
                current_bid = outcome.get('best_bid_price', 0)
                if current_bid < 0.10:
                    # bid < 0.10 = 호가창 공백 (매수자 없음) — 실제 가격 아님, 스킵
                    continue

                # peak_bid 갱신
                if current_bid > bet.get('peak_bid', 0):
                    bet['peak_bid'] = current_bid

                # ── 익절 트레일링 ───────────────────────────────────────────
                if TAKE_PROFIT_TRAIL > 0:
                    peak_bid   = bet.get('peak_bid', 0)
                    trail_line = peak_bid * (1 - TAKE_PROFIT_TRAIL)
                    in_profit  = current_bid > bet['ask']
                    if in_profit and peak_bid > bet['ask'] and current_bid < trail_line:
                        from py_clob_client_v2.order_builder.constants import SELL
                        log(f"TAKE-PROFIT  {bet['question'][:55]}\n"
                            f"             bid={current_bid:.3f} < trail={trail_line:.3f} "
                            f"(peak={peak_bid:.3f} × {1-TAKE_PROFIT_TRAIL:.0%})")
                        resp = place_live_order(bet['token_id'], current_bid, bet['tokens'], SELL)
                        if not resp:
                            log(f"TAKE-PROFIT-FAIL  SELL 실패 — 결과 대기")
                            break
                        recover = current_bid * bet['tokens']
                        profit  = recover - bet['bet']
                        bet.update({
                            'resolved': True, 'stop_loss_triggered': True,
                            'winner': 'take-profit', 'won': True,
                            'pnl': profit,
                        })
                        wins += 1
                        pnl  += profit
                        save_result(bet)
                        save_pending_bets(bets)
                        log(f"TAKE-PROFIT-EXIT  recovered=${recover:.2f}  profit=${profit:.2f}  secs_left={secs_left:.0f}")
                        send_telegram(
                            f"💰 <b>TAKE-PROFIT</b> {bet['question'][:45]}\n"
                            f"peak={peak_bid:.3f} → bid={current_bid:.3f} (trail {TAKE_PROFIT_TRAIL*100:.0f}%)\n"
                            f"회수: ${recover:.2f}  이익: +${profit:.2f}"
                        )
                        print_stats(wins, losses, pnl)
                        break

                stop_price  = bet['ask'] * STOP_LOSS_PCT
                if current_bid < stop_price:
                    from py_clob_client_v2.order_builder.constants import SELL
                    log(f"STOP-LOSS  {bet['question'][:55]}\n"
                        f"           bid={current_bid:.2f} < {stop_price:.2f} "
                        f"({STOP_LOSS_PCT*100:.0f}% of ask={bet['ask']:.2f})")
                    resp = place_live_order(bet['token_id'], current_bid, bet['tokens'], SELL)
                    if not resp:
                        # SELL 실패 (잔액 부족 등) → 포지션 유지, 자연 결과 대기
                        log(f"STOP-LOSS-FAIL  SELL 실패 — 포지션 유지하고 결과 대기  secs_left={secs_left:.0f}")
                        bet['stop_loss_triggered'] = True  # 재발동 방지
                        save_pending_bets(bets)
                        break
                    recover = current_bid * bet['tokens']
                    loss    = recover - bet['bet']
                    bet.update({
                        'resolved': True, 'stop_loss_triggered': True,
                        'winner': 'stop-loss', 'won': False,
                        'pnl': loss,
                    })
                    losses += 1
                    pnl    += loss
                    save_result(bet)
                    save_pending_bets(bets)
                    log(f"STOP-LOSS-EXIT  recovered=${recover:.2f}  loss=${loss:.2f}  secs_left={secs_left:.0f}")
                    send_telegram(
                        f"🛑 <b>STOP-LOSS</b> {bet['question'][:45]}\n"
                        f"bid={current_bid:.2f} < 손절선={stop_price:.2f}\n"
                        f"회수: ${recover:.2f}  손실: ${loss:.2f}"
                    )
                    print_stats(wins, losses, pnl)
                break

    # ── 3. Resolve pending bets ──────────────────────────────────────────────
    for slug, bet in list(bets.items()):
        if bet.get('resolved'):
            continue
        secs_since = (now - bet['end']).total_seconds()
        if secs_since < 10:
            continue

        if secs_since > GIVE_UP:
            log(f"SKIP  {slug} unresolved after {GIVE_UP}s — abandoning")
            bet['resolved'] = True
            save_pending_bets(bets)
            continue

        winner = check_resolution(bet.get('condition_id'), slug)
        if winner is None:
            last = warn_at.get(slug, -999)
            if secs_since - last >= 60:
                log(f"WAIT  {bet['question'][:50]}  ({secs_since:.0f}s since expiry)")
                warn_at[slug] = secs_since
            continue

        won        = (winner.strip().lower() == bet['outcome'].strip().lower())
        result_pnl = bet['profit_if_win'] if won else -bet['bet']
        wins      += int(won)
        losses    += int(not won)
        pnl       += result_pnl

        if won:
            consecutive_losses = 0
        else:
            consecutive_losses += 1
            _coin_last_loss[_slug_coin(slug)] = time.time()           # 방법2
            if consecutive_losses >= CONSEC_LOSS_COOLDOWN:             # 방법1
                _consec_cooldown_until = time.time() + CONSEC_LOSS_COOLDOWN_SECS
                log(f"COOLDOWN  {consecutive_losses}연패 → {CONSEC_LOSS_COOLDOWN_SECS}s 베팅 중단")

        bet.update({'resolved': True, 'winner': winner, 'won': won, 'pnl': result_pnl})
        save_result(bet)
        ml_update_result(bet.get('ml_row_id', -1), 'WIN' if won else 'LOSE', result_pnl)
        save_pending_bets(bets)

        emoji = '✓ WIN ' if won else '✗ LOSE'
        log(f"RESULT {emoji}  {bet['question'][:55]}\n"
            f"       bet={bet['outcome']}  winner={winner}  "
            f"net_pnl=${result_pnl:+.2f}")
        print_stats(wins, losses, pnl)
        tg_emoji = '✅' if won else '❌'
        if MODE == 'live':
            _live_bal = get_live_usdc()
            _bal_str = f"\n잔고: ${_live_bal:.2f}" if _live_bal is not None else ""
        elif PAPER_CAPITAL > 0:
            _bal_str = f"\n자본금: ${PAPER_CAPITAL + _csv_total_pnl():.2f}"
        else:
            _bal_str = ""
        send_telegram(
            f"{tg_emoji} <b>{'WIN' if won else 'LOSS'}</b>  {bet['question'][:50]}\n"
            f"예측: {bet['outcome']}  결과: {winner}\n"
            f"손익: ${result_pnl:+.2f}  누적: ${pnl:+.2f} ({wins}승 {losses}패)"
            + _bal_str
        )
        if not won and consecutive_losses >= CONSEC_LOSS_ALERT:
            send_telegram(
                f"⚠️ <b>연속 {consecutive_losses}연패 경고</b>\n"
                f"누적손익: ${pnl:+.2f}  ({wins}승 {losses}패)\n"
                f"시장 상황을 점검하세요."
            )

    time.sleep(INTERVAL)
