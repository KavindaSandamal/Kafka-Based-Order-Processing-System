import json
import io
import time
import random

from confluent_kafka import Producer
from fastavro import schemaless_writer


# 1. Load Avro schema
with open("../schemas/order.avsc", "r") as f:
    schema = json.load(f)

producer_conf = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_conf)

products = ["Laptop", "Monitor", "Smartphone", "Tablet", "Headphones"]
order_id = 1000


def avro_serialize(record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


if __name__ == "__main__":
    print("Starting producer… (Ctrl+C to stop)")
    try:
        while True:
            order_id += 1
            record = {
                "orderId": str(order_id),
                "product": random.choice(products),
                "price": float(f"{random.uniform(10, 500):.2f}")
            }

            value_bytes = avro_serialize(record)

            producer.produce(
                topic="orders",
                value=value_bytes,
                callback=delivery_report
            )
            producer.flush()

            print("📤 Produced:", record)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer…")
    finally:
        producer.flush()
