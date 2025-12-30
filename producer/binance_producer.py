import json
import time
import traceback

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt', 'bnbusdt', 'xrpusdt',
           'adausdt', 'dogeusdt', 'dotusdt', 'linkusdt', 'maticusdt']
TOPIC = "binance_trades"
# Kafka ayarları aynı kalıyor
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            acks=1,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
    except NoBrokersAvailable:
        print("Connect failed to Kafka, Trying again in 5 seconds...")
        time.sleep(5)
STREAMS = "/".join([f"{s}@trade" for s in SYMBOLS])
SOCKET = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"


def on_message(ws, message):
    try:
        data = json.loads(message)

        actual_trade = data.get('data', data)

        producer.send(TOPIC, actual_trade)
        print(f"Sent: {actual_trade.get('s')} @ {actual_trade.get('p')} quantity = {actual_trade.get('q')}")
    except Exception as e:
        traceback.print_exc()


def on_error(ws, error):
    print(f"### Connection Error: {error} ###")


def on_close(ws, close_status_code, close_msg):
    print(f"### Connection Closed: {close_msg} (Code: {close_status_code}) ###")
    print("Retrying in 5 seconds...")
    time.sleep(5)


def on_open(ws):
    print("Connected to Binance WebSocket")


def run_v2():
    ws = websocket.WebSocketApp(
        SOCKET,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    while True:
        try:
            run_v2()
        except Exception as e:
            print(f"Main Loop Error: {e}. Restarting...")
            time.sleep(5)
