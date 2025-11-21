import json
import time
import random
from confluent_kafka import Consumer, Producer, KafkaError
from io import BytesIO
import avro.schema
import avro.io

class OrderConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_file='schemas/order.avsc'):
        """Initialize Kafka consumer with Avro schema and retry logic"""
        
        # Consumer configuration
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'order-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        # Producer for retry and DLQ
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        
        # Load Avro schema
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        self.schema = avro.schema.parse(json.dumps(schema_dict))
        
        # Aggregation state
        self.total_price = 0.0
        self.order_count = 0
        self.running_average = 0.0
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 2  
        
    def deserialize_avro(self, avro_bytes):
        """Deserialize Avro bytes to order data"""
        bytes_reader = BytesIO(avro_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        return reader.read(decoder)
    
    def update_aggregation(self, price):
        """Update running average of prices"""
        self.total_price += price
        self.order_count += 1
        self.running_average = self.total_price / self.order_count
    
    def get_retry_count(self, headers):
        """Extract retry count from message headers"""
        if headers is None:
            return 0
        
        for key, value in headers:
            if key == 'retry_count':
                return int(value.decode('utf-8'))
        return 0
    
    def send_to_retry(self, key, value, headers, retry_count):
        """Send failed message to retry topic"""
        new_headers = [(k, v) for k, v in (headers or []) if k != 'retry_count']
        new_headers.append(('retry_count', str(retry_count).encode('utf-8')))
        
        self.producer.produce(
            topic='orders-retry',
            key=key,
            value=value,
            headers=new_headers
        )
        self.producer.flush()
        print(f"  Sent to retry queue (attempt {retry_count}/{self.max_retries})")
    
    def send_to_dlq(self, key, value, headers, error_msg):
        """Send permanently failed message to DLQ"""
        dlq_headers = list(headers or [])
        dlq_headers.append(('error', error_msg.encode('utf-8')))
        dlq_headers.append(('failed_at', str(int(time.time())).encode('utf-8')))
        
        self.producer.produce(
            topic='orders-dlq',
            key=key,
            value=value,
            headers=dlq_headers
        )
        self.producer.flush()
        print(f"  Sent to Dead Letter Queue: {error_msg}")
    
    def process_message(self, order_data):
        """Process order message - simulates potential failures"""
        # Simulate random processing failures (20% chance)
        if random.random() < 0.2:
            raise Exception("Simulated temporary processing failure")
        
        # Process successfully
        self.update_aggregation(order_data['price'])
        
        print(f"\n Processed Order {order_data['orderId']}")
        print(f"   Product: {order_data['product']}")
        print(f"   Price: ${order_data['price']:.2f}")
        print(f"    Running Average: ${self.running_average:.2f} ({self.order_count} orders)")
    
    def consume_messages(self):
        """Main consumer loop"""
        # Subscribe to both main and retry topics
        self.consumer.subscribe(['orders', 'orders-retry'])
        
        print("🎧 Consumer started. Waiting for messages...\n")
        print("=" * 60)
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f" Consumer error: {msg.error()}")
                        continue
                
                # Get retry count from headers
                retry_count = self.get_retry_count(msg.headers())
                
                try:
                    # Deserialize message
                    order_data = self.deserialize_avro(msg.value())
                    
                    # Process message
                    self.process_message(order_data)
                    
                    # Commit offset on success
                    self.consumer.commit(msg)
                    print("=" * 60)
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"\n  Processing failed: {error_msg}")
                    
                    if retry_count < self.max_retries:
                        # Send to retry queue
                        self.send_to_retry(
                            msg.key(), 
                            msg.value(), 
                            msg.headers(),
                            retry_count + 1
                        )
                        # Commit the message from current topic
                        self.consumer.commit(msg)
                        
                        # Wait before processing next message
                        time.sleep(self.retry_delay)
                    else:
                        # Max retries exceeded, send to DLQ
                        self.send_to_dlq(
                            msg.key(),
                            msg.value(),
                            msg.headers(),
                            f"Max retries exceeded: {error_msg}"
                        )
                        # Commit the message
                        self.consumer.commit(msg)
                    
                    print("=" * 60)
                
        except KeyboardInterrupt:
            print("\n\n Consumer interrupted by user")
        finally:
            self.close()
    
    def close(self):
        """Close consumer and producer"""
        print("\nFinal Statistics:")
        print(f"   Total Orders Processed: {self.order_count}")
        print(f"   Total Revenue: ${self.total_price:.2f}")
        print(f"   Average Order Value: ${self.running_average:.2f}")
        print("\nClosing consumer...")
        self.consumer.close()
        self.producer.flush()

if __name__ == '__main__':
    consumer = OrderConsumer()
    consumer.consume_messages()