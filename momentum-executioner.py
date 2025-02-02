from kafka import KafkaConsumer, KafkaAdminClient
import json
from binance.client import Client
from config import BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_CONFIG
from datetime import datetime, timedelta
import time
import threading
import csv
import os
from binance.exceptions import BinanceAPIException  # Import the exception

# Initialize the Binance client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Kafka configuration
kafka_broker = 'localhost:9094'  # Change to your Kafka broker address

# Set a static topic name
topic_name = "buy_signal"

# Function to check if the topic exists
def topic_exists(topic):
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
    topics = admin_client.list_topics()
    return topic in topics

# Ensure the buy_signal topic exists
if not topic_exists(topic_name):
    print(f"Topic '{topic_name}' does not exist. Exiting.")
    exit(1)

print(f"Subscribing to topic: {topic_name}")

time.sleep(5)  # Wait for the producer to start

# Capture the execution start time
execution_start_time = datetime.utcnow().timestamp()

print('starting at', str(execution_start_time))

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,  # Subscribe to the static buy_signal topic
    bootstrap_servers=kafka_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',  # Start reading from the latest messages
    enable_auto_commit=True,
    group_id='order_processor_group'
)

print('created consumer')
print(f"Listening for buy signals on topic: {topic_name}...")

# Dictionary to track trades
trades_count = {}

# Function to log trades to CSV
def log_trade(order_type, symbol, quantity, orderId, strategy):
    file_exists = os.path.isfile('trade-record.csv')
    with open('trade-record.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['OrderType', 'Symbol', 'Quantity', 'orderId', 'Strategy', 'Timestamp'])
        writer.writerow([order_type, symbol, quantity, orderId, strategy, datetime.now()])

# Function to submit a reverse order
def submit_reverse_order(symbol, quantity):
    # Sleep for 5 minutes
    time.sleep(300)
    try:
        # Submit a market sell order
        order = client.order_market_sell(
            symbol=symbol,
            quantity=quantity,
        )
        print(f"Submitted reverse sell order for {symbol}: {order}")
        log_trade('sell', symbol, quantity, order['orderId'], strategy)  # Log the sell order
    except BinanceAPIException as e:
        print(f"Failed to submit reverse sell order for {symbol}: {e.message}")  # Log the error

# Consume messages
try:
    for message in consumer:
        signal = message.value

        print(signal)
        
        # Check if the message has a valid timestamp
        message_timestamp = signal.get('timestamp')
        if message_timestamp is None:
            print("No timestamp in message. Ignoring...")
            continue

        # Ignore messages that are older than the execution start time
        if message_timestamp < execution_start_time:
            print("Ignoring message with timestamp earlier than execution start time.")
            continue
        
        if isinstance(signal, dict) and 'symbol' in signal and 'price' in signal:
            symbol = signal['symbol']
            price = signal['price']
            strategy = signal.get('strategy', 'unknown')  # Get strategy from the message
            usdt_amount, trading_enabled, max_trades_per_hour = TRADE_CONFIG.get(symbol, (0, False, 0))
            current_time = datetime.now()

            # Initialize trade count for the symbol if not already
            if symbol not in trades_count:
                trades_count[symbol] = []

            # Remove trades older than an hour
            trades_count[symbol] = [trade_time for trade_time in trades_count[symbol] if trade_time > current_time - timedelta(hours=1)]

            if trading_enabled and len(trades_count[symbol]) < max_trades_per_hour:
                # Calculate quantity based on USDT amount and price
                quantity = round(usdt_amount / price)  # Adjust precision as needed

                # Check quantity
                print(usdt_amount, quantity)
                
                try:
                    # Submit a market order
                    order = client.order_market_buy(
                        symbol=symbol,
                        quantity=quantity,
                        # Receive full price
                    )
                    print(f"Submitted buy order for {symbol}: {order}")

                    # Log the buy order
                    log_trade('buy', symbol, quantity, order['orderId'], strategy)  # Use strategy from message

                    # Record the trade time
                    trades_count[symbol].append(current_time)

                    # Start a thread to submit the reverse order
                    reverse_order_thread = threading.Thread(target=submit_reverse_order, args=(symbol, quantity))
                    reverse_order_thread.start()
                except BinanceAPIException as e:
                    print(f"Failed to submit buy order for {symbol}: {e.message}")  # Log the error
            else:
                if not trading_enabled:
                    print(f"Trading disabled for {symbol}. No order submitted.")
                else:
                    print(f"Max trades reached for {symbol} in the last hour. No order submitted.")

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    # Close the consumer connection
    consumer.close()