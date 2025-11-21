import json
from confluent_kafka import Consumer, KafkaError
from io import BytesIO
import avro.schema
import avro.io
from datetime import datetime

class DLQMonitor:
    def __init__(self, bootstrap_servers='localhost:9092', schema_file='schemas/order.avsc'):
        """Initialize DLQ monitor"""
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'dlq-monitor-group',
            'auto.offset.reset': 'earliest'
        })
        
        # Load Avro schema
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        self.schema = avro.schema.parse(json.dumps(schema_dict))
        
        self.dlq_count = 0
    
    def deserialize_avro(self, avro_bytes):
        """Deserialize Avro bytes to order data"""
        bytes_reader = BytesIO(avro_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.schema)
        return reader.read(decoder)
    
    def extract_headers(self, headers):
        """Extract header information"""
        header_dict = {}
        if headers:
            for key, value in headers:
                header_dict[key] = value.decode('utf-8')
        return header_dict
    
    def monitor_dlq(self):
        """Monitor Dead Letter Queue"""
        self.consumer.subscribe(['orders-dlq'])
        
        print("🔍 DLQ Monitor Started")
        print("=" * 80)
        print("Monitoring Dead Letter Queue for failed messages...\n")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f" Error: {msg.error()}")
                        continue
                
                self.dlq_count += 1
                
                # Deserialize message
                order_data = self.deserialize_avro(msg.value())
                headers = self.extract_headers(msg.headers())
                
                # Display failed message details
                print(f"\n DLQ Message #{self.dlq_count}")
                print(f"{'─' * 80}")
                print(f"Order ID:      {order_data['orderId']}")
                print(f"Product:       {order_data['product']}")
                print(f"Price:         ${order_data['price']:.2f}")
                
                if 'error' in headers:
                    print(f"Error Reason:  {headers['error']}")
                
                if 'failed_at' in headers:
                    failed_time = datetime.fromtimestamp(int(headers['failed_at']))
                    print(f"Failed At:     {failed_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                if 'retry_count' in headers:
                    print(f"Retry Count:   {headers['retry_count']}")
                
                print(f"{'─' * 80}")
                
                # Commit offset
                self.consumer.commit(msg)
                
        except KeyboardInterrupt:
            print(f"\n\n Total DLQ Messages: {self.dlq_count}")
            print(" Monitor stopped")
        finally:
            self.consumer.close()

if __name__ == '__main__':
    monitor = DLQMonitor()
    monitor.monitor_dlq()