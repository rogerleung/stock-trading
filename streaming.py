import websockets
import asyncio
import json

# Binance WebSocket API endpoint for BTC/USD
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Function to handle incoming WebSocket messages
async def listen_to_btcusd():
    async with websockets.connect(BINANCE_WS_URL) as websocket:
        print("Connected to Binance WebSocket. Listening for BTC/USD trades...")
        while True:
            try:
                # Receive real-time data
                message = await websocket.recv()
                data = json.loads(message)

                # Extract relevant information
                trade_time = data.get('T')  # Trade time (timestamp in milliseconds)
                price = data.get('p')       # Trade price
                quantity = data.get('q')    # Trade quantity
                print(f"Time: {trade_time}, Price: {price}, Quantity: {quantity}")

            except Exception as e:
                print(f"Error: {e}")
                break

# Run the WebSocket listener
async def main():
    await listen_to_btcusd()

# Start the event loop
if __name__ == "__main__":
    asyncio.run(main())