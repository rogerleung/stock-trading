import requests
import json
import time
from kafka import KafkaProducer
#from config import *
import time
import datetime
from datetime import date
from datetime import timedelta
import os
topic_name = 'coin'
servers = 'localhost:9092'
servers = 'kafka:9092'
LIMIT = 30
N = 5

#time.sleep(10)
print(servers)

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

# while (True):

#     try:
#         producer = KafkaProducer(bootstrap_servers=[servers])
#         break
#     except:
#         pass
#     time.sleep(1)

# def requestCoin(coin):
#     url = f'https://api.binance.com/api/v3/klines?symbol={coin}&interval=1m&limit={LIMIT}'
#     r = requests.get(url)
#     data = r.json()
#     return data

# def getCoins(coin_list):
#     data = {}
#     for coin in coin_list:
#         try:
#             coin_info = requestCoin(coin)
#             coin_aka = coin.replace('USDT', '')
#             data[coin_aka] = coin_info
            
#         except Exception as e:
#             print('getCoinsE:', e)
#         time.sleep(1)
#     return data

# def cleanCoin(data, name):
#     coin_data = {}
#     coin_data['name'] = name 
#     now = datetime.datetime.now()
#     timestart = int(now.timestamp())
#     coin_data['timestart'] = timestart
#    # coin_data['price'] = float(data[3])
#     coin_data['volume'] = float(data[7])
#    # coin_data['num'] = float(data[8])
#     coin_data
#     return coin_data

# def sendCoins(data, sleep_time = 5):
#     name_list = [x for x in data]
#     for i in range(len(data[name_list[0]])):
#         for name in name_list:
#             print(name)
#             coin = data[name][i]
#             t0 = (coin[0])
#             t1 = (coin[6])
#             volume = coin[7]
#             d0 = datetime.datetime.fromtimestamp(t0 / 1000 )
#             d1 = datetime.datetime.fromtimestamp(t1 / 1000 )
#             send_data = cleanCoin(coin, name)
#             print(d0, '-->', d1, 'volume: ', volume)  
#             producer.send(topic_name, json.dumps(send_data).encode('utf-8'))
#             time.sleep(sleep_time)
        
        
# def getCoinsList():
#     # url = 'https://api.binance.com/api/v3/ticker/24hr'
#     # r = requests.get(url)
#     # df = r.json()
#     # df = [coin for coin in df if coin["symbol"].endswith("USDT")]
#     # sdf = sorted(df, key = lambda x: float(x['lastPrice']), reverse = True)[:N]
#     # return [coin['symbol'] for coin in sdf]

#     return ['BTCUSDT', 'BNBUSDT', 'ETHUSDT', 'DOGEUSDT', 'XRPUSDT', 
#             'SHIBUSDT', 'ADAUSDT', 'ARBUSDT', 'MATICUSDT', 'SOLUSDT', 'PEPEUSDT']

# coin_list = getCoinsList()
# while True:
    
#     coins = getCoins(coin_list)
#     sendCoins(coins)


producer.close()

