# config.py

# Binance API credentials
BINANCE_API_KEY = ''  # Replace with your actual API key
BINANCE_API_SECRET = ''  # Replace with your actual API secret

# Trading configuration for each pair: (USDT amount, trading enabled, max trades per hour)
TRADE_CONFIG = {
    "BTCUSDT": (100, False, 1),   # 100 USDT, trading enabled, max 1 trades/hour
    "ETHUSDT": (50, False, 1),    # 50 USDT, trading enabled, max 1 trades/hour
    "BNBUSDT": (30, False, 1),    # 30 USDT, trading enabled, max 1 trades/hour
    "XRPUSDT": (20, False, 1),   # 20 USDT, trading disabled, max 1 trades/hour
    "ADAUSDT": (10, False, 1),    # 10 USDT, trading enabled, max 1 trades/hour
    "PEPEUSDT": (2, True, 10)     # 1 USDT, trading enabled, max 10 trades/hour
}