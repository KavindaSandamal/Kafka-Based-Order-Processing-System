from confluent_kafka import Consumer
import json

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-dlq-consumer",
    "auto.offset.reset": "earliest",
}

c = Consumer(conf)
c.subscribe(["orders-dlq"])

print("DLQ consumer started… (Ctrl+C to stop)")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        payload = json.loads(msg.value().decode("utf-8"))
        print("💀 DLQ message:", payload)
except KeyboardInterrupt:
    print("Stopping DLQ consumer…")
finally:
    c.close()
