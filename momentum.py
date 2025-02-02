from kafka import KafkaProducer
import json
from binance.client import Client
from datetime import datetime, timedelta
from config import BINANCE_API_KEY, BINANCE_API_SECRET, TRADE_CONFIG
import time

# Initialize the Binance client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# Kafka configuration
kafka_broker = 'localhost:9094'  # Change to your Kafka broker address
base_strategy_name = 'momentum'    # Base strategy name

time.sleep(1)  # Wait for the broker to start

print('starting')

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print('created producer')

# Function to get user input for KLINE interval
def get_kline_interval():
    print("Select a KLINE interval:")
    print("1. 1 minute")
    print("2. 5 minutes")
    print("3. 15 minutes")
    print("4. 30 minutes")
    print("5. 1 hour")
    print("6. 4 hours")
    print("7. 1 day")
    
    choice = input("Enter the number corresponding to your choice (1-7): ")
    intervals = {
        '1': Client.KLINE_INTERVAL_1MINUTE,
        '2': Client.KLINE_INTERVAL_5MINUTE,
        '3': Client.KLINE_INTERVAL_15MINUTE,
        '4': Client.KLINE_INTERVAL_30MINUTE,
        '5': Client.KLINE_INTERVAL_1HOUR,
        '6': Client.KLINE_INTERVAL_4HOUR,
        '7': Client.KLINE_INTERVAL_1DAY
    }
    
    return intervals.get(choice, Client.KLINE_INTERVAL_1MINUTE)  # Default to 1 minute

# Function to get user input for the number of consecutive periods
def get_consecutive_periods():
    periods = input("Enter the number of consecutive periods to check for increasing prices: ")
    try:
        periods = int(periods)
        if periods < 1:
            raise ValueError
    except ValueError:
        print("Invalid input. Defaulting to 3 periods.")
        periods = 3
    return periods

# Function to calculate the sleep time based on the interval and periods
def calculate_sleep_time(interval, periods):
    interval_mapping = {
        Client.KLINE_INTERVAL_1MINUTE: 60,  # 1 minute
        Client.KLINE_INTERVAL_5MINUTE: 300,  # 5 minutes
        Client.KLINE_INTERVAL_15MINUTE: 900,  # 15 minutes
        Client.KLINE_INTERVAL_30MINUTE: 1800,  # 30 minutes
        Client.KLINE_INTERVAL_1HOUR: 3600,  # 1 hour
        Client.KLINE_INTERVAL_4HOUR: 14400,  # 4 hours
        Client.KLINE_INTERVAL_1DAY: 86400  # 1 day
    }
    
    # Get the sleep time for the specific interval
    sleep_time = interval_mapping[interval]
    return sleep_time

# Function to get aggregated prices
def get_prices(symbol, interval, periods):
    # Determine the time range based on the interval
    now = datetime.utcnow()
    
    # Calculate the total minutes or days needed for the query
    if 'day' in interval:  # If it's a daily interval
        past = now - timedelta(days=periods)  # Fetch data for the number of days
    else:  # For minute, hourly, and 4-hour intervals
        # Determine the time delta based on the selected interval
        interval_mapping = {
            Client.KLINE_INTERVAL_1MINUTE: 1,
            Client.KLINE_INTERVAL_5MINUTE: 5,
            Client.KLINE_INTERVAL_15MINUTE: 15,
            Client.KLINE_INTERVAL_30MINUTE: 30,
            Client.KLINE_INTERVAL_1HOUR: 60,
            Client.KLINE_INTERVAL_4HOUR: 240
        }
        total_minutes = interval_mapping[interval] * periods
        past = now - timedelta(minutes=total_minutes)  # Fetch data for the required total minutes

    klines = client.get_historical_klines(symbol, interval, past.strftime("%d %b %Y %H:%M:%S"), now.strftime("%d %b %Y %H:%M:%S"))
    
    # Return the closing prices
    closing_prices = [float(kline[4]) for kline in klines]  # Closing price is at the 4th index
    return closing_prices

# Main loop to send signals
try:
    kline_interval = get_kline_interval()  # Get user-defined KLINE interval
    consecutive_periods = get_consecutive_periods()  # Get user-defined number of periods
    print(f"Using KLINE interval: {kline_interval} and checking for {consecutive_periods} consecutive periods.")

    # Create a strategy name including parameters
    strategy_name = f"{base_strategy_name}_{kline_interval}_{consecutive_periods}"

    # Set a static Kafka topic name
    output_topic = "buy_signal"

    while True:
        # Define the symbols you want to monitor
        symbols = TRADE_CONFIG

        for symbol in symbols:
            prices = get_prices(symbol, kline_interval, consecutive_periods)

            print(symbol, prices)
            
            if len(prices) >= consecutive_periods:
                # Check if the last 'consecutive_periods' closing prices are consecutively increasing
                if all(prices[-i] > prices[-(i + 1)] for i in range(1, consecutive_periods)):
                    current_price = prices[-1]
                    timestamp = datetime.utcnow().timestamp()
                    signal = {
                        'symbol': symbol,
                        'signal': 'buy',
                        'price': current_price,
                        'timestamp': timestamp,
                        'strategy': strategy_name  # Add strategy with parameters
                    }
                    producer.send(output_topic, value=signal)
                    print(f"Sent buy signal to topic '{output_topic}': {signal}")

        # Calculate and sleep for the next check based on the interval
        sleep_time = calculate_sleep_time(kline_interval, consecutive_periods)
        print(f"Sleeping for {sleep_time} seconds before the next check...")
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("Signal processor stopped.")
finally:
    producer.close()