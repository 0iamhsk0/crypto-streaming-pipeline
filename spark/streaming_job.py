
import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, min as spark_min, max as spark_max,
    sum as spark_sum, count as spark_count, min_by, max_by, to_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ---- Config -------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-ticks"

PG_CONFIG = dict(
    host="localhost",
    port=5433,
    dbname="crypto_streaming",
    user="crypto",
    password="crypto_pw",
)

# How many past windows (per symbol) form the "normal" baseline for z-score.
# 20 windows @ 1-min each = roughly the last 20 minutes of trading behavior.
ZSCORE_LOOKBACK_WINDOWS = 20
ZSCORE_ANOMALY_THRESHOLD = 3.0

# Schema of the JSON the producer publishes (see producer/binance_producer.py)
tick_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_id", LongType()),
    StructField("trade_time", LongType()),   # epoch millis, exchange event time
    StructField("ingested_at", StringType()),
])


def compute_zscore_and_upsert(batch_df, batch_id):
    """
    Runs once per micro-batch (every `trigger` interval - see main()).
    batch_df holds one row per (symbol, window) that has new/updated data
    in this batch.
    """
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    for row in rows:
        symbol = row["symbol"]

        pct_change = (
            ((row["close_price"] - row["open_price"]) / row["open_price"]) * 100
            if row["open_price"] else 0.0
        )

        # Pull recent finalized windows for this symbol to build the baseline.
        cur.execute(
            """
            SELECT volume, pct_change
            FROM market_windows
            WHERE symbol = %s
            ORDER BY window_start DESC
            LIMIT %s
            """,
            (symbol, ZSCORE_LOOKBACK_WINDOWS),
        )
        history = cur.fetchall()

        volume_zscore = None
        price_zscore = None
        is_anomaly = False

        # Need a minimum sample size before z-scores mean anything -
        # with only 1-2 points, "3 standard deviations" is meaningless noise.
        if len(history) >= 5:
            volumes = [float(h[0]) for h in history]
            pct_changes = [float(h[1]) for h in history if h[1] is not None]

            vol_mean = sum(volumes) / len(volumes)
            vol_std = (sum((v - vol_mean) ** 2 for v in volumes) / len(volumes)) ** 0.5

            if pct_changes:
                pc_mean = sum(pct_changes) / len(pct_changes)
                pc_std = (sum((p - pc_mean) ** 2 for p in pct_changes) / len(pct_changes)) ** 0.5
            else:
                pc_mean, pc_std = 0.0, 0.0

            if vol_std > 0:
                volume_zscore = (float(row["volume"]) - vol_mean) / vol_std
            if pc_std > 0:
                price_zscore = (pct_change - pc_mean) / pc_std

            is_anomaly = (
                (volume_zscore is not None and abs(volume_zscore) > ZSCORE_ANOMALY_THRESHOLD)
                or (price_zscore is not None and abs(price_zscore) > ZSCORE_ANOMALY_THRESHOLD)
            )

        cur.execute(
            """
            INSERT INTO market_windows
                (symbol, window_start, window_end, open_price, high_price,
                 low_price, close_price, volume, trade_count, pct_change,
                 volume_zscore, price_zscore, is_anomaly)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                window_end    = EXCLUDED.window_end,
                open_price    = EXCLUDED.open_price,
                high_price    = EXCLUDED.high_price,
                low_price     = EXCLUDED.low_price,
                close_price   = EXCLUDED.close_price,
                volume        = EXCLUDED.volume,
                trade_count   = EXCLUDED.trade_count,
                pct_change    = EXCLUDED.pct_change,
                volume_zscore = EXCLUDED.volume_zscore,
                price_zscore  = EXCLUDED.price_zscore,
                is_anomaly    = EXCLUDED.is_anomaly
            """,
            (
                symbol, row["window"]["start"], row["window"]["end"],
                row["open_price"], row["high_price"], row["low_price"],
                row["close_price"], row["volume"], row["trade_count"],
                pct_change, volume_zscore, price_zscore, is_anomaly,
            ),
        )

        flag = "  <-- ANOMALY" if is_anomaly else ""
        print(f"[batch {batch_id}] {symbol} | O:{row['open_price']:.2f} H:{row['high_price']:.2f} "
              f"L:{row['low_price']:.2f} C:{row['close_price']:.2f} vol:{row['volume']:.4f}{flag}")

    cur.close()
    conn.close()


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "4")  # small local job, no need for the 200 default
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), tick_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("trade_time") / 1000))
    )

    windowed = (
        parsed
        # Tolerate ticks arriving up to 30s late (network jitter between
        # Binance -> producer -> Kafka). After 30s past a window's end,
        # Spark stops accepting updates for it and drops late stragglers.
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("symbol"),
        )
        .agg(
            # min_by/max_by give us price-at-earliest-time and
            # price-at-latest-time - i.e. true open/close, not just
            # min/max price (which would be wrong: high/low, not open/close).
            min_by(col("price"), col("event_time")).alias("open_price"),
            max_by(col("price"), col("event_time")).alias("close_price"),
            spark_max(col("price")).alias("high_price"),
            spark_min(col("price")).alias("low_price"),
            spark_sum(col("quantity")).alias("volume"),
            spark_count(col("trade_id")).alias("trade_count"),
        )
    )

    query = (
        windowed.writeStream
        .foreachBatch(compute_zscore_and_upsert)
        # "update" mode: emit a window's current state on every trigger,
        # even before it's finalized by the watermark. This makes the
        # dashboard feel live (numbers update mid-window) instead of
        # only refreshing once a minute. Safe here because we UPSERT
        # (ON CONFLICT), so repeated updates to the same window just
        # overwrite, they don't duplicate.
        .outputMode("update")
        .option("checkpointLocation", "./_checkpoints/crypto_streaming")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
