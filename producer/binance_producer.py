
import json
import logging
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("binance_producer")

# ---- Config -----------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-ticks"

# Trading pairs to stream. These are the 5 highest-volume/most-liquid pairs
# on Binance - enough to make the dashboard feel like a real multi-asset
# system, without turning it into 15 tabs of noise. Adding more later is a
# one-line change here (and in dashboard/app.py's SYMBOLS list) - the
# ingestion/processing layers don't care how many symbols flow through them,
# since Spark already groups by symbol regardless of how many exist.
SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# Binance combined stream URL, e.g. .../stream?streams=btcusdt@trade/ethusdt@trade
STREAM_PATH = "/".join(f"{s}@trade" for s in SYMBOLS)
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_PATH}"

RECONNECT_DELAY_SECONDS = 5

# ---- Kafka producer ----------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    # Small batching so we don't hammer the broker on every single tick.
    linger_ms=100,
    acks="all",
)


def on_message(ws, message):
    """Parse a Binance trade event and publish it to Kafka."""
    try:
        payload = json.loads(message)
        trade = payload.get("data", {})

        if trade.get("e") != "trade":
            return  # ignore non-trade events, if any slip through

        record = {
            "symbol": trade["s"],                 # e.g. "BTCUSDT"
            "price": float(trade["p"]),
            "quantity": float(trade["q"]),
            "trade_id": trade["t"],
            "trade_time": trade["T"],              # epoch ms, exchange-side event time
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(KAFKA_TOPIC, key=record["symbol"], value=record)
        log.info("Published %s @ %s (qty %s)", record["symbol"], record["price"], record["quantity"])

    except (KeyError, ValueError, json.JSONDecodeError) as e:
        log.warning("Skipped malformed message: %s | error: %s", message[:200], e)
    except KafkaError as e:
        log.error("Failed to publish to Kafka: %s", e)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    log.warning("WebSocket closed (code=%s, msg=%s). Reconnecting in %ss...",
                close_status_code, close_msg, RECONNECT_DELAY_SECONDS)


def on_open(ws):
    log.info("Connected to Binance stream: %s", SYMBOLS)


def run_forever():
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=180, ping_timeout=10)
        time.sleep(RECONNECT_DELAY_SECONDS)


if __name__ == "__main__":
    try:
        run_forever()
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
        producer.flush()
        producer.close()
