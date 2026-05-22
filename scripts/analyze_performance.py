#!/usr/bin/env python3
"""
코인별 최적 설정값 분석 스크립트
사용법: python3 scripts/analyze_performance.py [--min-trades N] [--days N] [--csv]
"""
import sqlite3
import csv
import argparse
from pathlib import Path
from collections import defaultdict
import datetime

DB_PATH  = Path(__file__).parent.parent / "logs" / "sniper-ml.db"
CSV_PATH = Path(__file__).parent.parent / "logs" / "sniper-results.csv"
FEE_RATE = 0.0175
MIN_TRADES = 10

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--min-trades", type=int, default=MIN_TRADES, help="최소 거래 수 (기본: 10)")
    p.add_argument("--days", type=int, default=0, help="최근 N일 데이터만 분석 (기본: 전체)")
    p.add_argument("--coin", type=str, default="", help="특정 코인만 분석")
    p.add_argument("--csv", action="store_true", help="sniper-results.csv도 함께 분석")
    return p.parse_args()

def load_csv_rows(days=0, coin_filter=""):
    """CSV에서 실거래 데이터 로드 (momentum 등 일부 컬럼 없음)"""
    rows = []
    if not CSV_PATH.exists():
        return rows
    cutoff = None
    if days > 0:
        cutoff = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    with open(CSV_PATH) as f:
        for r in csv.DictReader(f):
            slug = r["slug"]
            coin = slug.split("-")[0]
            if coin_filter and coin != coin_filter.lower():
                continue
            if cutoff and r["time"][:10] < cutoff:
                continue
            interval = "15m" if "15m" in slug else "5m"
            won = r["won"].strip().lower() == "true"
            try:
                dt = datetime.datetime.strptime(r["time"], "%Y-%m-%d %H:%M:%S")
                # ET = UTC-4 or UTC-5; approximate with UTC-4
                hour_et = (dt.hour - 4) % 24
            except Exception:
                hour_et = -1
            rows.append({
                "coin": coin, "interval": interval,
                "result": "WIN" if won else "LOSE",
                "ask": float(r["ask"]),
                "momentum_pct": None,
                "direction": r["bet_outcome"],
                "secs_left": None,
                "candle_consistency": None,
                "hour_et": hour_et,
                "net_pnl": float(r["net_pnl"]),
                "source": "csv",
            })
    return rows

def ev(win_rate, avg_ask, bet=50):
    """기대값 계산"""
    net_win = bet / avg_ask - bet - bet * FEE_RATE
    return win_rate * net_win - (1 - win_rate) * bet

def bar(val, max_val=100, width=20):
    filled = int(val / max_val * width)
    return "█" * filled + "░" * (width - filled)

def analyze(con, args):
    where = "decision='BET' AND result IS NOT NULL"
    if args.days > 0:
        where += f" AND ts >= datetime('now', '-{args.days} days')"
    if args.coin:
        where += f" AND coin='{args.coin.lower()}'"

    db_rows = con.execute(f"""
        SELECT coin, interval, result, ask, momentum_pct, direction,
               secs_left, candle_consistency, hour_et, net_pnl
        FROM decisions WHERE {where}
        ORDER BY ts
    """).fetchall()

    # DB rows를 dict로 변환
    rows = [{"coin":r[0],"interval":r[1],"result":r[2],"ask":r[3],
              "momentum_pct":r[4],"direction":r[5],"secs_left":r[6],
              "candle_consistency":r[7],"hour_et":r[8],"net_pnl":r[9],
              "source":"db"} for r in db_rows]

    # CSV 데이터 병합
    if args.csv:
        csv_rows = load_csv_rows(args.days, args.coin)
        rows = csv_rows + rows  # CSV가 더 오래된 데이터이므로 앞에
        if csv_rows:
            print(f"  [DB: {len(db_rows)}건 + CSV: {len(csv_rows)}건 합산]")

    if not rows:
        print("데이터 없음")
        return

    total = len(rows)
    wins = sum(1 for r in rows if r["result"] == "WIN")
    pnl  = sum(r["net_pnl"] for r in rows)
    print(f"\n{'='*60}")
    print(f" 전체 요약: {total}건  {wins}승 {total-wins}패  "
          f"승률={wins/total*100:.1f}%  손익={pnl:+.2f}")
    print(f"{'='*60}")

    # ── 1. 코인 × 인터벌별 ───────────────────────────────────────
    print("\n[ 코인 × 인터벌별 성과 ]")
    print(f"{'코인':<12} {'거래':>4} {'승률':>6} {'평균ask':>8} {'손익':>8}  신뢰도")
    print("-" * 56)
    bucket = defaultdict(lambda: {"w":0,"l":0,"pnl":0.0,"asks":[]})
    for r in rows:
        k = (r["coin"], r["interval"])
        bucket[k]["w" if r["result"]=="WIN" else "l"] += 1
        bucket[k]["pnl"] += r["net_pnl"]
        bucket[k]["asks"].append(r["ask"])
    for k in sorted(bucket):
        d = bucket[k]; n = d["w"]+d["l"]
        if n < args.min_trades: continue
        wr = d["w"]/n; aask = sum(d["asks"])/len(d["asks"])
        flag = "✓" if n>=30 else "△" if n>=15 else "?"
        print(f"{k[0]+'-'+k[1]:<12} {n:>4} {wr*100:>5.1f}%  {aask:>7.4f}  {d['pnl']:>+7.2f}  {flag}({n}건)")

    # ── 2. ask 구간별 ────────────────────────────────────────────
    bands = [(0.83,0.87),(0.87,0.89),(0.89,0.91),(0.91,0.93),(0.93,0.97)]
    print("\n[ ask 구간별 승률 (전체 코인) ]")
    print(f"{'구간':<12} {'거래':>4} {'승률':>6} {'EV/50$':>8}  바")
    print("-" * 52)
    for lo, hi in bands:
        sub = [r for r in rows if lo <= r["ask"] < hi]
        if len(sub) < args.min_trades: continue
        w = sum(1 for r in sub if r["result"]=="WIN"); n = len(sub)
        wr = w/n; aask = sum(r["ask"] for r in sub)/n
        print(f"{lo:.2f}~{hi:.2f}     {n:>4} {wr*100:>5.1f}%  {ev(wr,aask):>+7.2f}  {bar(wr*100,100,15)}")

    # ── 3. 코인별 ask × 방향 상세 ────────────────────────────────
    coins = sorted(set(r["coin"] for r in rows))
    for coin in coins:
        cr = [r for r in rows if r["coin"]==coin]
        if len(cr) < args.min_trades: continue
        print(f"\n[ {coin.upper()} — ask 구간 × 방향 ]")
        print(f"{'조건':<20} {'거래':>4} {'승률':>6} {'EV/50$':>8}")
        print("-"*42)
        for lo, hi in bands:
            for dirn in ["Up", "Down"]:
                sub = [r for r in cr if lo<=r["ask"]<hi and r["direction"]==dirn]
                if len(sub) < 3: continue
                w = sum(1 for r in sub if r["result"]=="WIN"); n=len(sub)
                wr = w/n; aask = sum(r["ask"] for r in sub)/n
                mark = "★" if wr>=0.97 else "✓" if wr>=0.90 else ""
                print(f"{dirn} {lo:.2f}~{hi:.2f}        {n:>4} {wr*100:>5.1f}%  {ev(wr,aask):>+7.2f}  {mark}")

    # ── 4. 모멘텀 강도별 (DB 데이터만 해당) ──────────────────────
    mom_rows = [r for r in rows if r["momentum_pct"] is not None]
    if mom_rows:
        print("\n[ 모멘텀 강도별 승률 (DB 데이터) ]")
        print(f"{'구간':<14} {'거래':>4} {'승률':>6} {'EV/50$':>8}")
        print("-"*38)
        for lo, hi in [(0,0.15),(0.15,0.20),(0.20,0.25),(0.25,0.35),(0.35,1.0)]:
            sub = [r for r in mom_rows if lo <= abs(r["momentum_pct"]) < hi]
            if len(sub) < 3: continue
            w = sum(1 for r in sub if r["result"]=="WIN"); n=len(sub)
            wr = w/n; aask = sum(r["ask"] for r in sub)/n
            print(f"{lo:.2f}~{hi:.2f}%      {n:>4} {wr*100:>5.1f}%  {ev(wr,aask):>+7.2f}")

    # ── 5. 잔여시간별 (DB 데이터만 해당) ─────────────────────────
    sl_rows = [r for r in rows if r["secs_left"] is not None]
    if sl_rows:
        print("\n[ 잔여시간별 승률 (DB 데이터) ]")
        print(f"{'구간(초)':<14} {'거래':>4} {'승률':>6} {'EV/50$':>8}")
        print("-"*38)
        for lo, hi in [(40,100),(100,150),(150,175),(175,200),(200,250),(250,350)]:
            sub = [r for r in sl_rows if lo <= r["secs_left"] < hi]
            if len(sub) < 3: continue
            w = sum(1 for r in sub if r["result"]=="WIN"); n=len(sub)
            wr = w/n; aask = sum(r["ask"] for r in sub)/n
            print(f"{lo:>3}~{hi:<3}초        {n:>4} {wr*100:>5.1f}%  {ev(wr,aask):>+7.2f}")

    # ── 6. 최적 조건 추천 ────────────────────────────────────────
    print(f"\n{'='*60}")
    print(" 최적 조건 추천 (승률 90%↑, EV 양수, 5건 이상)")
    print(f"{'='*60}")
    for coin in coins:
        best = []
        for lo, hi in bands:
            for dirn in ["Up", "Down"]:
                sub = [r for r in rows if r["coin"]==coin and lo<=r["ask"]<hi and r["direction"]==dirn]
                if len(sub) < 5: continue
                w = sum(1 for r in sub if r["result"]=="WIN"); n=len(sub)
                wr = w/n; aask = sum(r["ask"] for r in sub)/n
                ev_val = ev(wr, aask)
                if wr >= 0.90 and ev_val > 0:
                    best.append((wr, n, dirn, lo, hi, ev_val))
        if best:
            best.sort(reverse=True)
            print(f"\n  {coin.upper()}:")
            for wr, n, dirn, lo, hi, ev_val in best[:3]:
                print(f"    {dirn} ask={lo:.2f}~{hi:.2f}  →  승률={wr*100:.1f}%  EV={ev_val:+.2f}  ({n}건)")

    # ── 7. 시간대별 승률 ──────────────────────────────────────────
    print("\n[ ET 시간대별 승률 ]")
    hour_bucket = defaultdict(lambda:{"w":0,"l":0})
    for r in rows:
        h = r["hour_et"]
        hour_bucket[h]["w" if r["result"]=="WIN" else "l"] += 1
    print(f"{'시간(ET)':<10} {'거래':>4} {'승률':>6}  바")
    print("-"*38)
    for h in sorted(hour_bucket):
        d = hour_bucket[h]; n=d["w"]+d["l"]
        if n < 3: continue
        wr = d["w"]/n
        print(f"{h:02d}:00~{(h+1)%24:02d}:00  {n:>4} {wr*100:>5.1f}%  {bar(wr*100,100,15)}")

    print(f"\n{'='*60}")
    print(f" 데이터: {total}건 / 코인당 평균 {total//max(len(coins),1)}건")
    print(f" 신뢰도: ★ 97%↑  ✓ 90%↑  △ 15건 미만  ? 10건 미만")
    print(f" 권장 신뢰 기준: 코인당 30건 이상")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    args = parse_args()
    con = sqlite3.connect(DB_PATH)
    try:
        analyze(con, args)
    finally:
        con.close()

