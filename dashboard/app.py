"""
Day 3 — Streamlit dashboard.

Reads live OHLCV + anomaly windows from Postgres, auto-refreshes, and shows
AI-generated commentary (batched across all symbols in one API call - see
ai/commentary.py for why) plus a cross-symbol overview.

Run:
    streamlit run dashboard\\app.py

Requires GEMINI_API_KEY set in your environment (or a .env file - see
.env.example). Without it, charts still work; the commentary box will show
a placeholder instead of AI text (see ai/commentary.py's fallback).

VISUAL DESIGN NOTE: coin badges use real logo artwork from
github.com/spothq/cryptocurrency-icons, licensed CC0-1.0 (public domain) -
safe to use, modify, and redistribute with no attribution required. Files
are stored locally in dashboard/assets/, not fetched at runtime, so there's
no network dependency and no ambiguity about licensing (unlike scraping
logos from a random site or an exchange's brand kit, which may carry
usage restrictions).
"""

import os
import sys
import time
import warnings

# pandas warns that psycopg2 connections aren't officially "tested" for
# pd.read_sql (it prefers SQLAlchemy engines). This is a known, harmless
# warning for psycopg2 - our queries work correctly, it's just noise.
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

# Allow importing ai/commentary.py when run via `streamlit run dashboard/app.py`
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import psycopg2
import plotly.graph_objects as go
import streamlit as st

from ai.commentary import generate_batch_commentary

PG_CONFIG = dict(
    host="localhost",
    port=5433,   # matches the remapped port in docker-compose.yml
    dbname="crypto_streaming",
    user="crypto",
    password="crypto_pw",
)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
REFRESH_SECONDS = 15
LOOKBACK_WINDOWS = 60  # roughly the last hour of 1-minute windows

# Now using gemini-3.5-flash (see ai/commentary.py for why 2.5-flash was
# dropped) - its free tier is 15 requests/min, 1,500/day, far more generous
# than the ~20/day we hit on 2.5-flash. One batched call per regen cycle
# still costs exactly 1 request regardless of symbol count.
COMMENTARY_REGEN_SECONDS = 180

# Real coin logos - CC0-licensed (public domain), sourced from
# github.com/spothq/cryptocurrency-icons and stored locally in
# dashboard/assets/. Not fetched at runtime: no network dependency, no
# licensing ambiguity, and they render instantly.
ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")

COIN_META = {
    "BTCUSDT": {"name": "Bitcoin",  "file": "btc.svg", "c1": "#F7931A"},
    "ETHUSDT": {"name": "Ethereum", "file": "eth.svg", "c1": "#627EEA"},
    "BNBUSDT": {"name": "BNB",      "file": "bnb.svg", "c1": "#F3BA2F"},
    "SOLUSDT": {"name": "Solana",   "file": "sol.svg", "c1": "#66F9A1"},
    "XRPUSDT": {"name": "XRP",      "file": "xrp.svg", "c1": "#23292F"},
}

st.set_page_config(page_title="Crypto Streaming Analytics", layout="wide", page_icon="📈")


@st.cache_data
def load_svg(filename: str) -> str:
    path = os.path.join(ASSETS_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def inject_custom_css():
    st.markdown("""
    <style>
    .coin-badge {
        width: 92px; height: 92px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        margin-left: auto; padding: 14px; box-sizing: border-box;
        background: rgba(255,255,255,0.03);
        box-shadow: 0 0 24px 6px var(--glow-color, rgba(255,255,255,0.25));
        border: 2px solid rgba(255,255,255,0.15);
    }
    .coin-badge svg { width: 100%; height: 100%; }
    .commentary-card {
        border-radius: 10px; padding: 14px 18px; margin-top: 8px;
        border-left: 4px solid var(--accent, #00CC96);
        background: linear-gradient(90deg, rgba(255,255,255,0.05), rgba(255,255,255,0.0));
        display: flex; align-items: flex-start; gap: 12px;
    }
    .commentary-avatar {
        font-size: 22px; line-height: 1; margin-top: 2px;
    }
    .commentary-text { font-size: 15px; line-height: 1.4; }
    .overview-badge-row { display: flex; gap: 18px; margin-bottom: 6px; }
    .mini-badge {
        width: 46px; height: 46px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        padding: 7px; box-sizing: border-box;
        background: rgba(255,255,255,0.03);
        box-shadow: 0 0 14px 3px var(--glow-color, rgba(255,255,255,0.2));
        border: 1.5px solid rgba(255,255,255,0.15);
    }
    .mini-badge svg { width: 100%; height: 100%; }
    </style>
    """, unsafe_allow_html=True)


def coin_badge_html(symbol: str, size: str = "large") -> str:
    meta = COIN_META.get(symbol)
    if not meta:
        return ""
    css_class = "coin-badge" if size == "large" else "mini-badge"
    glow = meta["c1"] + "aa"
    svg_markup = load_svg(meta["file"])
    return f'<div class="{css_class}" style="--glow-color:{glow};">{svg_markup}</div>'


@st.cache_resource
def get_connection():
    return psycopg2.connect(**PG_CONFIG)


def load_windows(symbol: str, limit: int = LOOKBACK_WINDOWS) -> pd.DataFrame:
    conn = get_connection()
    query = """
        SELECT window_start, window_end, open_price, high_price, low_price,
               close_price, volume, trade_count, pct_change, volume_zscore,
               price_zscore, is_anomaly
        FROM market_windows
        WHERE symbol = %s
        ORDER BY window_start DESC
        LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(symbol, limit))
    return df.sort_values("window_start")


def load_latest_all_symbols() -> pd.DataFrame:
    """One row per symbol: its most recent window. Powers the Overview tab
    and the AI commentary batch call - cheap Postgres queries, not API calls."""
    rows = []
    for symbol in SYMBOLS:
        df = load_windows(symbol, limit=1)
        if not df.empty:
            row = df.iloc[-1].to_dict()
            row["symbol"] = symbol
            rows.append(row)
    return pd.DataFrame(rows)


def refresh_commentary_if_due(latest_df: pd.DataFrame):
    """Regenerates the batched AI commentary at most once per
    COMMENTARY_REGEN_SECONDS. On failure, keeps the previous good commentary
    instead of overwriting it with an error - a stale-but-real insight beats
    a visible error message on every subsequent rerun."""
    now = time.time()
    due = (
        "commentary" not in st.session_state
        or now - st.session_state.get("commentary_ts", 0) > COMMENTARY_REGEN_SECONDS
    )
    if not due or latest_df.empty:
        return

    windows = {row["symbol"]: row.to_dict() for _, row in latest_df.iterrows()}
    result = generate_batch_commentary(windows)

    prior = st.session_state.get("commentary", {})
    merged = dict(prior)
    for sym, text in result.items():
        if not text.startswith("(unavailable"):
            merged[sym] = text
        elif sym not in merged:
            merged[sym] = text  # nothing to fall back to yet

    st.session_state["commentary"] = merged
    st.session_state["commentary_ts"] = now


def commentary_card_html(symbol: str, text: str, is_anomaly: bool) -> str:
    accent = "#EF553B" if is_anomaly else "#00CC96"
    avatar = "⚠️" if is_anomaly else "🤖"
    return (
        f'<div class="commentary-card" style="--accent:{accent};">'
        f'<div class="commentary-avatar">{avatar}</div>'
        f'<div class="commentary-text"><b>{symbol}</b> — {text}</div>'
        f'</div>'
    )


def render_overview(latest_df: pd.DataFrame):
    st.subheader("Overview — all symbols, latest window")

    if latest_df.empty:
        st.info("No data yet — is the Spark job (spark/streaming_job.py) running?")
        return

    badges = "".join(
        f'<div style="text-align:center;">{coin_badge_html(sym, "small")}'
        f'<div style="font-size:11px; margin-top:4px; opacity:0.8;">{sym}</div></div>'
        for sym in SYMBOLS if sym in latest_df["symbol"].values
    )
    st.markdown(f'<div class="overview-badge-row">{badges}</div>', unsafe_allow_html=True)

    colors = ["#EF553B" if v < 0 else "#00CC96" for v in latest_df["pct_change"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=latest_df["symbol"], y=latest_df["pct_change"],
        marker_color=colors,
        text=[f"{v:.2f}%" for v in latest_df["pct_change"]],
        textposition="outside",
    ))
    fig.update_layout(
        title="% Change — latest 1-minute window, per symbol",
        yaxis_title="% Change", height=350,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    display_cols = ["symbol", "close_price", "pct_change", "volume", "trade_count", "is_anomaly"]
    st.dataframe(
        latest_df[display_cols].rename(columns={
            "symbol": "Symbol", "close_price": "Last Close", "pct_change": "% Change",
            "volume": "Volume", "trade_count": "Trades", "is_anomaly": "Anomaly?",
        }),
        use_container_width=True, hide_index=True,
    )

    commentary = st.session_state.get("commentary", {})
    if commentary:
        st.caption(f"🤖 AI commentary (regenerates roughly every {COMMENTARY_REGEN_SECONDS // 60} min to respect free-tier API limits):")
        latest_by_symbol = {row["symbol"]: row for _, row in latest_df.iterrows()}
        for sym in SYMBOLS:
            if sym in commentary:
                is_anom = bool(latest_by_symbol.get(sym, {}).get("is_anomaly", False))
                st.markdown(commentary_card_html(sym, commentary[sym], is_anom), unsafe_allow_html=True)


def render_symbol(symbol: str):
    df = load_windows(symbol)

    if df.empty:
        st.info(f"No data yet for {symbol} — is the Spark job (spark/streaming_job.py) running?")
        return

    latest = df.iloc[-1]
    meta = COIN_META.get(symbol, {"name": symbol})

    header_col, badge_col = st.columns([4, 1])
    with header_col:
        st.subheader(f"{meta['name']} ({symbol})")
        st.caption(f"Last updated window: {latest['window_start']}")
    with badge_col:
        st.markdown(coin_badge_html(symbol, "large"), unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Last Close", f"${latest['close_price']:.2f}", f"{latest['pct_change']:.2f}%")
    col2.metric("1-min Volume", f"{latest['volume']:.4f}")
    col3.metric("Trades this window", int(latest["trade_count"]))
    col4.metric("Anomaly?", "YES" if latest["is_anomaly"] else "No")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["window_start"], y=df["close_price"],
        mode="lines+markers", name="Close price",
        line=dict(color=meta.get("c1", "#00CC96")),
    ))
    anomalies = df[df["is_anomaly"]]
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies["window_start"], y=anomalies["close_price"],
            mode="markers", name="Anomaly",
            marker=dict(color="red", size=12, symbol="x"),
        ))
    fig.update_layout(
        title=f"{symbol} — Close Price (last {len(df)} windows)",
        xaxis_title="Time", yaxis_title="Price (USDT)",
        height=350, margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    vol_fig = go.Figure()
    colors = ["red" if a else meta.get("c1", "#636EFA") for a in df["is_anomaly"]]
    vol_fig.add_trace(go.Bar(x=df["window_start"], y=df["volume"], marker_color=colors))
    vol_fig.update_layout(
        title=f"{symbol} — Volume per window",
        height=200, margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(vol_fig, use_container_width=True)

    commentary = st.session_state.get("commentary", {}).get(symbol)
    if commentary:
        st.markdown(
            commentary_card_html(symbol, commentary, bool(latest["is_anomaly"])),
            unsafe_allow_html=True,
        )


def main():
    inject_custom_css()

    st.title("📈 Real-Time Crypto Market Analytics Pipeline")
    st.caption(
        "Live Binance trade data → Kafka/Redpanda → Spark Structured Streaming "
        "→ Postgres → AI-generated insights"
    )

    latest_df = load_latest_all_symbols()
    refresh_commentary_if_due(latest_df)

    tab_labels = ["Overview"] + SYMBOLS
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        render_overview(latest_df)

    for tab, symbol in zip(tabs[1:], SYMBOLS):
        with tab:
            render_symbol(symbol)

    st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s. Last refresh: {pd.Timestamp.now().strftime('%H:%M:%S')}")
    time.sleep(REFRESH_SECONDS)
    st.rerun()


if __name__ == "__main__":
    main()
