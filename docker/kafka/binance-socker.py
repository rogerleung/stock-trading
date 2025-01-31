import websocket
import json
from kafka import KafkaProducer

# List of top 20 USDT pairs
top_usdt_pairs = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
    # "SOLUSDT", "DOGEUSDT", "DOTUSDT", "MATICUSDT", "TRXUSDT",
    # "LTCUSDT", "BCHUSDT", "LINKUSDT", "ETCUSDT", "AVAXUSDT",
    # "SHIBUSDT", "UNIUSDT", "XLMUSDT", "FILUSDT", "VETUSDT"
]

# Kafka configuration
kafka_broker = 'localhost:9092'  # Change to your Kafka broker address
kafka_topic = 'binance_tickers'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=[kafka_broker])

# Binance WebSocket endpoint for the ticker
base_url = "wss://stream.binance.com:9443/ws"

def on_message(ws, message):
    data = json.loads(message)
    print(f"Symbol: {data['s']}, Price: {data['c']}, Volume: {data['v']}")
    
    # Log the message and send to Kafka
    try:
        producer.send(kafka_topic, value=data)
        producer.flush()  # Ensure the message is sent before proceeding
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print(f"### closed ### Code: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    # Subscribe to the ticker for each pair
    for pair in top_usdt_pairs:
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{pair.lower()}@ticker"],
            "id": 1
        }))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(base_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()