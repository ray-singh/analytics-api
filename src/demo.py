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
# Construct the necessary time series
ts = td.time_series(
    symbol="AAPL",
    interval="1min",
    outputsize=10,
    timezone="America/New_York",
)

# Returns pandas.DataFrame
ts.as_json()
print(ts)