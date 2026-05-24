#!/usr/bin/env python3
"""
claude_estimator.py — Polymarket binary outcome 확률 추정 (Claude LLM)
2026-05-24 사용자 6864 A+B (Anthropic Polymarket 가이드 통합).

설계:
- expiry-sniper.py 의 BET 결정 직전에 호출되는 optional probability layer
- 입력: market question + outcome + current ask + coin + secs_left
- 출력: (probability_estimate 0~1, reasoning) or (None, error_reason)
- Graceful fail: Claude 호출 실패 시 (None, ...) → 기존 로직으로 그대로 진행
- Cache: 동일 slug 30초 dedup (API 비용 절감)

활성화:
- env CLAUDE_LAYER_ENABLED=true → 활성
- env CLAUDE_LAYER_ENABLED 미설정/false → 호출 시 (None, "disabled") 즉시 반환

비용 통제:
- Anthropic API max_tokens=300 (제한)
- 30초 cache
- 단일 호출 ~$0.001 (Claude Sonnet 4.6 기준)
- 일 100건 베팅 시 월 ~$3
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────
CLAUDE_LAYER_ENABLED = os.environ.get('CLAUDE_LAYER_ENABLED', 'false').lower() in ('1', 'true', 'yes')
CLAUDE_CACHE_TTL = int(os.environ.get('CLAUDE_CACHE_TTL_SECS', '30'))
CLAUDE_MAX_TOKENS = int(os.environ.get('CLAUDE_MAX_TOKENS', '300'))
CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'claude-sonnet-4-6')
CLAUDE_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

# ── In-memory cache (slug → (timestamp, prob, reason)) ─
_cache: dict[str, tuple[float, Optional[float], str]] = {}


def _cache_get(slug: str) -> Optional[Tuple[Optional[float], str]]:
    entry = _cache.get(slug)
    if not entry:
        return None
    ts, prob, reason = entry
    if time.time() - ts > CLAUDE_CACHE_TTL:
        _cache.pop(slug, None)
        return None
    return (prob, reason)


def _cache_put(slug: str, prob: Optional[float], reason: str) -> None:
    _cache[slug] = (time.time(), prob, reason)
    # Cleanup oldest if cache > 200 entries
    if len(_cache) > 200:
        oldest = min(_cache.items(), key=lambda x: x[1][0])
        _cache.pop(oldest[0], None)


def _strip_md_fence(s: str) -> str:
    """```json...``` 마크다운 펜스 제거 (trading-system llm_scorer 패턴 재사용)."""
    s = s.strip()
    s = re.sub(r'^```(?:json|JSON)?\s*\n?', '', s)
    s = re.sub(r'\n?```\s*$', '', s)
    return s.strip()


def _parse_claude_response(resp: str) -> Tuple[Optional[float], str]:
    """Claude JSON 응답에서 (probability, reasoning) 추출.

    예상 형식: {"probability": 0.72, "reasoning": "Strong upward momentum + ..."}
    """
    if not resp:
        return None, "empty response"
    cleaned = _strip_md_fence(resp)

    # 1. JSON 파싱 시도 (greedy)
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group())
            prob = data.get("probability")
            if prob is not None:
                prob = max(0.0, min(1.0, float(prob)))
                reasoning = str(data.get("reasoning", ""))[:200].strip()
                return prob, reasoning or "no reasoning"
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.debug(f"[claude_est] JSON parse fail: {e}")

    # 2. 부분 JSON 정규식 추출
    prob_m = re.search(r'"probability"\s*:\s*([\d.]+)', cleaned)
    reason_m = re.search(r'"reasoning"\s*:\s*"([^"]*?)(?:"|$)', cleaned, re.DOTALL)
    if prob_m:
        try:
            prob = max(0.0, min(1.0, float(prob_m.group(1))))
            reasoning = reason_m.group(1)[:200].strip() if reason_m else "응답 잘림"
            return prob, reasoning
        except ValueError:
            pass

    return None, "parse failed"


def _call_anthropic_api(prompt: str, system: str) -> Optional[str]:
    """Anthropic API 직접 호출 (Claude Sonnet 4.6 기본).

    pmarket-arb 는 trading-system 의 LLM Guard infra 와 분리됨 → 직접 호출.
    실패 시 None 반환 (caller 가 graceful fail 처리).
    """
    if not CLAUDE_API_KEY:
        return None
    try:
        import urllib.request
        body = json.dumps({
            "model": CLAUDE_MODEL,
            "max_tokens": CLAUDE_MAX_TOKENS,
            "system": system,
            "messages": [{"role": "user", "content": prompt}],
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        content = data.get("content", [])
        if content and isinstance(content, list):
            return content[0].get("text", "")
    except Exception as e:
        logger.debug(f"[claude_est] Anthropic API fail: {e}")
        return None
    return None


def estimate_probability(
    slug: str,
    question: str,
    outcome: str,
    market_price: float,
    coin: str,
    secs_left: float,
    binance_features: Optional[dict] = None,
) -> Tuple[Optional[float], str]:
    """Polymarket binary outcome 확률 추정.

    Args:
        slug: market slug (cache key)
        question: market question
        outcome: 우리가 베팅하는 outcome ("Yes" / "No" / coin direction)
        market_price: current ask (0~1)
        coin: 'btc' / 'eth' / 'sol' 등
        secs_left: market expiry 까지 남은 초
        binance_features: 옵션 — 현재 가격, 모멘텀, 변동성 dict (binance_data 참고)

    Returns:
        (probability_estimate 0~1, reasoning)
        - 비활성/실패 시 (None, error_reason) — caller 는 기존 로직으로 fallback

    호출 패턴:
        prob, reason = estimate_probability(slug, q, outcome, ask, coin, secs)
        if prob is not None:
            edge = prob - ask  # positive = 우리에게 유리
            if edge < MIN_EDGE_PCT:
                continue  # skip
    """
    if not CLAUDE_LAYER_ENABLED:
        return None, "disabled"

    # Cache check
    cached = _cache_get(slug)
    if cached:
        return cached

    # Build prompt
    feature_str = ""
    if binance_features:
        feature_str = (
            f"\n현재 시장 데이터:\n"
            f"- 현재가: {binance_features.get('price', '?')}\n"
            f"- 5분 모멘텀: {binance_features.get('slope_5m', '?')}%/분\n"
            f"- 변동성: {binance_features.get('volatility', '?')}%\n"
        )

    prompt = (
        f"Polymarket binary 예측 시장 분석:\n\n"
        f"Question: {question}\n"
        f"우리가 베팅하려는 outcome: {outcome}\n"
        f"현재 시장가격 (ask): {market_price:.3f} (= 시장 추정 {market_price*100:.1f}%)\n"
        f"종목: {coin.upper()}\n"
        f"남은 시간: {secs_left:.0f}초"
        f"{feature_str}\n"
        f"이 outcome 이 실제로 발생할 확률을 0~1 사이로 추정하세요.\n"
        f"시장 가격과 다를 수 있습니다 (있다면 그게 edge 입니다).\n\n"
        f"반드시 JSON 만 답변:\n"
        f'{{"probability": <0~1 소수>, "reasoning": "<1-2문장 근거>"}}'
    )

    system = (
        "당신은 단기 가격 예측 시장 분석가입니다. "
        "주어진 모멘텀/변동성/시간 데이터를 바탕으로 outcome 확률을 객관적으로 추정합니다. "
        "시장 가격을 그대로 따르지 말고 독립적 추정을 제공하세요. "
        "반드시 JSON 형식으로만 답변하세요."
    )

    resp = _call_anthropic_api(prompt, system)
    if not resp:
        _cache_put(slug, None, "api unavailable")
        return None, "api unavailable"

    prob, reasoning = _parse_claude_response(resp)
    _cache_put(slug, prob, reasoning)
    return prob, reasoning


# ── CLI test ─────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(name)s] %(message)s")

    # Quick test (requires ANTHROPIC_API_KEY)
    if not CLAUDE_API_KEY:
        print("ANTHROPIC_API_KEY not set — cannot test live")
        # Test parsing only
        test_resp = '{"probability": 0.72, "reasoning": "Strong upward momentum"}'
        prob, reason = _parse_claude_response(test_resp)
        print(f"Parse test: prob={prob} reason={reason!r}")
        sys.exit(0)

    # Live test
    os.environ['CLAUDE_LAYER_ENABLED'] = 'true'
    prob, reason = estimate_probability(
        slug="test-btc-5m-2026",
        question="Will BTC go up in the next 5 minutes?",
        outcome="Up",
        market_price=0.85,
        coin="btc",
        secs_left=180,
        binance_features={"price": 67000, "slope_5m": 0.05, "volatility": 0.3},
    )
    print(f"Estimate: prob={prob} reason={reason}")
