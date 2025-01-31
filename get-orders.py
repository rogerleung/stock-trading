from binance.client import Client
from datetime import datetime
import config  # Assuming your API keys are in a config file

# Initialize the Binance client
client = Client(config.BINANCE_API_KEY, config.BINANCE_API_SECRET)

def get_today_orders(symbol):
    """Fetch all orders executed for a given symbol today."""
    # Get the start and end of today
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    # Convert to milliseconds
    start_time = int(today_start.timestamp() * 1000)
    end_time = int(today_end.timestamp() * 1000)

    # Fetch all orders for the symbol
    try:
        orders = client.get_all_orders(symbol=symbol, startTime=start_time, endTime=end_time)
        return orders
    except Exception as e:
        print(f"Error fetching orders for {symbol}: {e}")
        return []

def get_all_symbols():
    """Fetch all trading pairs available on Binance."""
    try:
        exchange_info = client.get_exchange_info()
        symbols = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

if __name__ == "__main__":
    # Fetch all trading pairs
    symbols = get_all_symbols()

    for symbol in symbols:
        orders = get_today_orders(symbol)
        if orders:
            print(f"Orders executed for {symbol} today:")
            for order in orders:
                print(order)
        else:
            print(f"No orders executed for {symbol} today.")