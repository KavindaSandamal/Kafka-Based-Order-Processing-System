import json
import random
import time
from confluent_kafka import Producer
from io import BytesIO
import avro.schema
import avro.io

class OrderProducer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_file='schemas/order.avsc'):
        """Initialize Kafka producer with Avro schema"""
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'order-producer'
        })
        
        # Load Avro schema
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        self.schema = avro.schema.parse(json.dumps(schema_dict))
        
        self.products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 
                        'Mouse', 'Headphones', 'Webcam', 'Speaker', 'Charger']
    
    def serialize_avro(self, order_data):
        """Serialize order data using Avro"""
        writer = avro.io.DatumWriter(self.schema)
        bytes_writer = BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(order_data, encoder)
        return bytes_writer.getvalue()
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f' Message delivery failed: {err}')
        else:
            print(f' Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def produce_order(self, order_id):
        """Produce a single order message"""
        order_data = {
            'orderId': str(order_id),
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 1000.0), 2),
            'timestamp': int(time.time() * 1000)
        }
        
        # Serialize to Avro
        avro_bytes = self.serialize_avro(order_data)
        
        # Send to Kafka
        self.producer.produce(
            topic='orders',
            key=str(order_id).encode('utf-8'),
            value=avro_bytes,
            callback=self.delivery_report
        )
        
        # Flush to ensure delivery
        self.producer.poll(0)
        
        print(f" Produced: Order {order_data['orderId']} - {order_data['product']} - ${order_data['price']}")
        return order_data
    
    def produce_batch(self, num_orders=10, interval=1):
        """Produce multiple orders with interval"""
        print(f"\n Starting to produce {num_orders} orders...\n")
        
        for i in range(1, num_orders + 1):
            self.produce_order(1000 + i)
            time.sleep(interval)
        
        # Wait for all messages to be delivered
        print("\n Flushing remaining messages...")
        self.producer.flush()
        print(" All messages sent!\n")
    
    def close(self):
        """Close the producer"""
        self.producer.flush()

if __name__ == '__main__':
    producer = OrderProducer()
    
    try:
        # Produce 20 orders with 2 second intervals
        producer.produce_batch(num_orders=20, interval=2)
    except KeyboardInterrupt:
        print("\n  Producer interrupted by user")
    finally:
        producer.close()
        print(" Producer closed!")