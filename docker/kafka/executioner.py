from kafka import KafkaConsumer
import json
from binance.client import Client
from config import BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_CONFIG
from datetime import datetime, timedelta
import time
import threading

# Initialize the Binance client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Kafka configuration
kafka_broker = 'localhost:9092'  # Change to your Kafka broker address
input_topic = 'buy_signals'        # The topic to consume buy signals from

time.sleep(60) # Wait for the producer to start

# Create a Kafka consumer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_broker,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='order_processor_group'
)

print(f"Listening for buy signals on topic '{input_topic}'...")

# Dictionary to track trades
trades_count = {}

# Function to submit a reverse order
def submit_reverse_order(symbol, quantity):
    # Sleep for 5 minutes
    time.sleep(300)
    # Submit a market sell order
    order = client.order_market_sell(
        symbol=symbol,
        quantity=quantity,
        newOrderRespType=FULL
    )
    print(f"Submitted reverse sell order for {symbol}: {order}")

# Consume messages
try:
    for message in consumer:
        signal = message.value

        print(signal)
        
        if isinstance(signal, dict) and 'symbol' in signal and 'price' in signal:
            symbol = signal['symbol']
            price = signal['price']
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
                
                # Submit a market order
                order = client.order_market_buy(
                    symbol=symbol,
                    quantity=quantity,
                    # Receive full price
                    newOrderRespType=FULL
                )
                print(f"Submitted buy order for {symbol}: {order}")

                # Record the trade time
                trades_count[symbol].append(current_time)

                # Start a thread to submit the reverse order
                reverse_order_thread = threading.Thread(target=submit_reverse_order, args=(symbol, quantity))
                reverse_order_thread.start()
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