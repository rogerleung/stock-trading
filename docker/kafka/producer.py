from kafka import KafkaProducer
import json
from binance.client import Client
from datetime import datetime, timedelta
from config import BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_CONFIG
import time

# Initialize the Binance client
# BINANCE_API_KEY = 'your_api_key'  # Replace with your actual API key
# BINANCE_API_SECRET = 'your_api_secret'  # Replace with your actual API secret
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Kafka configuration
kafka_broker = 'localhost:9092'  # Change to your Kafka broker address
output_topic = 'buy_signals'       # The topic to send buy signals to

time.sleep(60) # Wait for the broker to start

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to get aggregated minute prices
def get_minute_prices(symbol):
    # Get the last 60 minutes of price data
    now = datetime.utcnow()
    past = now - timedelta(minutes=60)
    
    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, past.strftime("%d %b %Y %H:%M:%S"), now.strftime("%d %b %Y %H:%M:%S"))
    
    # Return the closing prices
    closing_prices = [float(kline[4]) for kline in klines]  # Closing price is at index 4
    return closing_prices

# Main loop to send signals
try:
    while True:
        # Define the symbols you want to monitor
        symbols = TRADE_CONFIG

        for symbol in symbols:
            prices = get_minute_prices(symbol)
            
            if len(prices) >= 3:
                # Calculate the average of the last three closing prices
                avg_price = sum(prices[-3:]) / 3
                current_price = prices[-1]

                # Simple buy signal logic: Buy if current price is less than average
                if current_price < avg_price:
                    timestamp = datetime.utcnow().timestamp()
                    signal = {
                        'symbol': symbol,
                        'signal': 'buy',
                        'price': current_price,
                        'timestamp': timestamp
                    }
                    producer.send(output_topic, value=signal)
                    print(f"Sent buy signal: {signal}")

        # Sleep for a minute before the next check
        time.sleep(60)

except KeyboardInterrupt:
    print("Signal processor stopped.")
finally:
    producer.close()