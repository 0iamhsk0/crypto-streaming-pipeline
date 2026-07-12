

import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-ticks"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",   # only show ticks from now onward
    group_id=None,                # no consumer group — just observing
)

print(f"Listening on topic '{KAFKA_TOPIC}'... (Ctrl+C to stop)\n")

for message in consumer:
    tick = message.value
    print(f"{tick['symbol']:10s} | price={tick['price']:>12} | qty={tick['quantity']:>10} "
          f"| trade_id={tick['trade_id']}")
