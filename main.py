import streamlit as st
from binance.client import Client
from datetime import datetime, timedelta
import pandas as pd
import time
import threading
from config import BINANCE_API_KEY, BINANCE_API_SECRET
from config import TRADE_CONFIG
import seaborn as sns
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh

latest_x_day = 2

st. set_page_config(layout="wide")

# update every 5 mins
st_autorefresh(interval=.1 * 60 * 1000, key="dataframerefresh")

# Streamlit App
st.title("Executed Orders from Binance")

# Initialize the Binance client
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

col1, col2 = st.columns(2)

def get_last_7_days_orders(symbol):
    """Fetch all executed orders for the last 7 days for a specific symbol."""
    seven_days_ago = datetime.now() - timedelta(days=latest_x_day)
    seven_days_ago_timestamp = int(seven_days_ago.timestamp() * 1000)  # Convert to milliseconds

    orders = client.get_my_trades(symbol=symbol)

    # Filter orders for the last 7 days
    last_7_days_orders = [
        order for order in orders if order['time'] >= seven_days_ago_timestamp
    ]
    
    return last_7_days_orders

def get_last_7_days_orders(symbol):
    """Fetch all executed orders for the last 7 days for a specific symbol."""
    seven_days_ago = datetime.now() - timedelta(days=latest_x_day)
    seven_days_ago_timestamp = int(seven_days_ago.timestamp() * 1000)  # Convert to milliseconds

    orders = client.get_my_trades(symbol=symbol)

    # Filter orders for the last 7 days
    last_7_days_orders = [
        order for order in orders if order['time'] >= seven_days_ago_timestamp
    ]
    
    return last_7_days_orders

def get_last_7_days_orders_from_config(trade_config):
    """Fetch all executed orders for the last 7 days for all symbols in the trade config."""
    all_last_7_days_orders = []

    for symbol in trade_config.keys():
        last_7_days_orders = get_last_7_days_orders(symbol)
        for order in last_7_days_orders:
            order['symbol'] = symbol  # Add symbol to each order
            all_last_7_days_orders.append(order)
    
    # Convert to DataFrame
    if all_last_7_days_orders:
        return pd.DataFrame(all_last_7_days_orders)
    else:
        return pd.DataFrame()  # Return empty DataFrame if no orders found

def fetch_orders():
    """Fetch and update executed orders every 30 seconds."""
    df = get_last_7_days_orders_from_config(TRADE_CONFIG)
    # Update session state in a Streamlit-friendly way
    df['time'] = pd.to_datetime(df['time'], unit='ms')

    # Convert 'executedQty' to float
    df['qty_new'] = df['qty'].astype(float)
    df['price_new'] = df['price'].astype(float)

    # Assume PNL is calculated based on executedQty and price
    # This is a placeholder logic; adjust it based on your actual PNL calculation
    df['pnl'] = df.apply(lambda row: row['qty_new'] * row['price_new'] if not row['isBuyer'] else -row['qty_new'] * row['price_new'], axis=1)
    df['cumulative_pnl'] = df['pnl'].cumsum()

    return df

with col1:
    st.subheader("detailed order information")
    st.dataframe(fetch_orders())

with col2:
    st.subheader("cumulative PNL by symbol")
    # Create a timeseries chart
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=fetch_orders(), x='time', y='cumulative_pnl')
    plt.title('Cumulative PNL Over Time')
    plt.xlabel('Time')
    plt.ylabel('Cumulative PNL')
    plt.xticks(rotation=45)
    plt.tight_layout()
    st.pyplot(plt)

st.write("This app will update the executed orders every 10 seconds.")