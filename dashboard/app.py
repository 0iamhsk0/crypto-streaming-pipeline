"""
Reads live OHLCV + anomaly windows from Postgres, auto-refreshes, and shows
AI-generated commentary (batched across all symbols in one API call - see
ai/commentary.py for why) plus a cross-symbol overview.

Run:
    streamlit run dashboard\\app.py

Requires GEMINI_API_KEY set in your environment (or a .env file - see
.env.example). Without it, charts still work; the commentary box will show
a placeholder instead of AI text (see ai/commentary.py's fallback).
"""

import os
import sys
import time

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

COMMENTARY_REGEN_SECONDS = 180

st.set_page_config(page_title="Crypto Streaming Analytics", layout="wide")


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

    # Only overwrite entries that actually succeeded; keep prior good text
    # for any symbol whose call failed this cycle.
    prior = st.session_state.get("commentary", {})
    merged = dict(prior)
    for sym, text in result.items():
        if not text.startswith("(unavailable"):
            merged[sym] = text
        elif sym not in merged:
            merged[sym] = text  # nothing to fall back to yet

    st.session_state["commentary"] = merged
    st.session_state["commentary_ts"] = now


def render_overview(latest_df: pd.DataFrame):
    st.subheader("Overview — all symbols, latest window")

    if latest_df.empty:
        st.info("No data yet — is the Spark job (spark/streaming_job.py) running?")
        return

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
        st.caption("🤖 AI commentary (regenerates roughly every 15 min to respect free-tier API limits):")
        for sym in SYMBOLS:
            if sym in commentary:
                st.write(f"**{sym}**: {commentary[sym]}")


def render_symbol(symbol: str):
    df = load_windows(symbol)

    if df.empty:
        st.info(f"No data yet for {symbol} — is the Spark job (spark/streaming_job.py) running?")
        return

    latest = df.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Last Close", f"${latest['close_price']:.2f}", f"{latest['pct_change']:.2f}%")
    col2.metric("1-min Volume", f"{latest['volume']:.4f}")
    col3.metric("Trades this window", int(latest["trade_count"]))
    col4.metric("Anomaly?", "YES" if latest["is_anomaly"] else "No")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["window_start"], y=df["close_price"],
        mode="lines+markers", name="Close price",
        line=dict(color="#00CC96"),
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
    colors = ["red" if a else "#636EFA" for a in df["is_anomaly"]]
    vol_fig.add_trace(go.Bar(x=df["window_start"], y=df["volume"], marker_color=colors))
    vol_fig.update_layout(
        title=f"{symbol} — Volume per window",
        height=200, margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(vol_fig, use_container_width=True)

    commentary = st.session_state.get("commentary", {}).get(symbol)
    if commentary:
        if latest["is_anomaly"]:
            st.warning(f"🤖 {commentary}")
        else:
            st.info(f"🤖 {commentary}")


def main():
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
