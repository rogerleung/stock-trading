from kafka import KafkaConsumer
import json

# Kafka configuration
kafka_broker = 'kafka:9092'  # Change to your Kafka broker address
kafka_topic = 'binance_tickers'   # The topic to consume from

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=True,        # Commit offsets automatically
    group_id='binance_consumer_group'  # Consumer group ID
)

print(f"Listening for messages on topic '{kafka_topic}'...")

# Consume messages
try:
    for message in consumer:
        # Print the raw message for debugging
        print(f"Raw message: {message.value}")
        
        # Deserialize the message value
        data = message.value
        if isinstance(data, dict):  # Check if data is a dictionary
            print(f"Symbol: {data['s']}, Price: {data['c']}, Volume: {data['v']}")
        else:
            print("Received message is not a dictionary.")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    # Close the consumer connection
    consumer.close()