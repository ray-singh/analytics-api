import os
import time
import ssl
import certifi
from twelvedata import TDClient
import datetime
from dotenv import load_dotenv
messages_history = []


def on_event(e):
    # do whatever is needed with data
    if e["event"] == "price":
        print(e['symbol'], e['price'], e['timestamp'])
    messages_history.append(e)

load_dotenv()
td = TDClient(apikey=os.getenv("TWELVEDATA_API_KEY"))
ws = td.websocket(symbols="AAPL", on_event=on_event, ssl_context=ssl.create_default_context(cafile=certifi.where()))
ws.subscribe('AAPL')
ws.connect()
while True:
    ws.heartbeat()
    time.sleep(10)