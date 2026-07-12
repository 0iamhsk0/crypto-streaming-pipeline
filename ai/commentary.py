
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
                max_output_tokens=500,
                temperature=0.3,
                response_mime_type="application/json",
            ),
        )
        return json.loads(response.text)
    except Exception as e:
        # Print the full detail to the terminal running Streamlit, for
        # debugging - but keep the UI message short and clean for viewers.
        print(f"[Gemini API error] {type(e).__name__}: {e}")
        reason = type(e).__name__
        return {sym: f"(unavailable: {reason})" for sym in windows}
