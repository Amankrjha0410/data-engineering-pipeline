import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Configurations
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "orders"

# Create Producer
producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    value_serializer = lambda v : json.dumps(v).encode("utf-8"),
    linger_ms=10,  # batching for efficiency
    batch_size = 16384
)

# DATA GENERATOR FUNCTION
def generate_order():
    return{
        "order_id": random.randint(1000,9999),
        "user_id": random.randint(1,100),
        "product_id": random.randint(1,50),
        "amount": random.randint(100,50000),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# MAIN LOOP
if __name__ == "__main__":
    # Generate and send orders indefinitely
    print("Starting Kafka Producer....")
    while True:
        order = generate_order()
        producer.send(TOPIC_NAME, value=order)
        print(f"Sent: {order}")
        time.sleep(1)

    # # Generate and send 1000 orders
    # print("Starting Kafka Producer....")
    # total_orders = 1000
    # for _ in range(total_orders):
    #     order = generate_order()
    #     producer.send(TOPIC_NAME, value=order)
    #     print(f"Sent: {order}")
        
    # producer.flush()
    # producer.close()
    # print(f"Finished sending {total_orders} orders.")
