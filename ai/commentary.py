"""
Day 3 — AI commentary layer.

Turns each symbol's latest OHLCV + anomaly stats into a short, plain-English
market observation using Google's Gemini API (free tier - no credit card
needed).

IMPORTANT DESIGN CHOICE - batched into ONE call for all symbols:
As of July 2026, Google began blocking gemini-2.5-flash for some projects
ahead of its official October 2026 shutdown (a known rollout issue on
Google's developer forums), and its free tier was in any case a very
restrictive 5 requests/minute and ~20 requests/day. This project now uses
gemini-3.5-flash instead, whose free tier is far more generous (15
requests/min, 1,500/day). Even so, batching all 5 symbols into ONE request
per cycle (instead of one call per symbol) is still the right design: it's
a 5x reduction in calls for identical output, and keeps the pipeline
resilient if free-tier limits tighten again in the future - a deliberate
choice, not just "call it less often."

Still deliberately a single, well-structured API call - not an agent
framework. There's no multi-step reasoning or tool use happening here
(just "summarize these numbers"), so a direct call is both simpler AND
more honest about what's actually being done.
"""

import json

from dotenv import load_dotenv
load_dotenv()  # loaded here, not just in app.py - so this module works
               # correctly regardless of import order in whatever calls it

from google import genai
from google.genai import types

client = genai.Client()  # reads GEMINI_API_KEY from the environment

MODEL = "gemini-3.5-flash"

SYSTEM_PROMPT = """You are a market commentary assistant for a live crypto trading dashboard.
You will be given OHLCV data for the latest 1-minute window of SEVERAL symbols.
For EACH symbol, write ONE short, plain-English sentence (max 20 words)
describing what happened - suitable for a non-technical viewer glancing at
a dashboard.

Rules:
- Be specific: mention direction (up/down), rough magnitude, and volume if notable.
- If is_anomaly is true for a symbol, open that symbol's sentence with
  "Unusual activity:" and explain why (volume spike or price move).
- Never invent numbers not present in the input.
- No hedging language ("might", "could suggest") - state it plainly. This is
  descriptive, not predictive.
- Do not give investment advice or make predictions about future price movement.

Return ONLY a valid JSON object mapping each symbol to its sentence, e.g.:
{"BTCUSDT": "...", "ETHUSDT": "..."}
No markdown formatting, no code fences, no extra text - just the raw JSON object.
"""


def generate_batch_commentary(windows: dict) -> dict:
    """
    windows: dict of {symbol: window_dict}, one entry per symbol, where each
    window_dict has keys open_price, high_price, low_price, close_price,
    volume, pct_change, is_anomaly, volume_zscore, price_zscore (matching a
    row from the market_windows table).

    Returns dict of {symbol: commentary_string}. On failure, returns a short
    per-symbol placeholder instead of raising - a flaky API call should never
    take down the whole dashboard.
    """
    symbols_block = "\n\n".join(
        f"Symbol: {sym}\n"
        f"Open: {w['open_price']}\nHigh: {w['high_price']}\nLow: {w['low_price']}\n"
        f"Close: {w['close_price']}\nVolume: {w['volume']}\n"
        f"% Change: {w['pct_change']:.3f}\nIs anomaly: {w['is_anomaly']}\n"
        f"Volume z-score: {w.get('volume_zscore')}\nPrice z-score: {w.get('price_zscore')}"
        for sym, w in windows.items()
    )
    user_prompt = f"Latest windows:\n\n{symbols_block}"

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                # gemini-3.5-flash has "thinking" on by default (medium
                # level), and those internal reasoning tokens count AGAINST
                # max_output_tokens - not separate from it. Without capping
                # thinking low, the model can burn the entire token budget
                # on internal reasoning before writing any actual JSON,
                # producing a truncated response that fails to parse. This
                # task is simple summarization, not complex reasoning, so
                # LOW thinking is appropriate - not a workaround, a correct
                # setting for the task.
                thinking_config=types.ThinkingConfig(
                    thinking_level=types.ThinkingLevel.LOW
                ),
                # Generous headroom for 5 symbols' worth of JSON output,
                # even with some thinking tokens still consumed.
                max_output_tokens=2000,
                response_mime_type="application/json",
                # temperature/top_p/top_k are no longer recommended for
                # Gemini 3.x models - Google's guidance is to leave these
                # at their defaults, which are already tuned for this
                # model family's reasoning behavior.
            ),
        )
        # Use raw_decode instead of json.loads: even with response_mime_type
        # set to JSON, the model can occasionally append trailing content
        # after a complete, valid JSON object (e.g. a stray newline-separated
        # remark). raw_decode parses just the first valid JSON value and
        # ignores anything after it, rather than failing on "Extra data".
        decoder = json.JSONDecoder()
        text = response.text.strip()
        result, _ = decoder.raw_decode(text)
        return result
    except Exception as e:
        # Print the full detail to the terminal running Streamlit, for
        # debugging - but keep the UI message short and clean for viewers.
        print(f"[Gemini API error] {type(e).__name__}: {e}")
        reason = type(e).__name__
        return {sym: f"(unavailable: {reason})" for sym in windows}
