import websocket
import threading
import json
import Key
import uuid
import requests
from coinbase.rest import RESTClient
import time
from collections import deque
import Source as s
import urllib3
from requests.exceptions import ConnectionError, ReadTimeout
import random
import datetime
from datetime import datetime, timezone
import numpy as np

s_data = ''
t_data = ''
xrp_data = ''
account_USD = ''

xrp_imbalance = 0.0        # smoothed imbalance your bot will read
xrp_buy_vol = 0.0          # optional rolling totals
xrp_sell_vol = 0.0
imbalance_window = deque(maxlen=50)   # smoothing window (~1–2 seconds)
flow_imb = 0.0

prev_imbalance = 0
imbalance_delta = 0

# order book
order_book_bids = {}  # price -> size
order_book_asks = {}  # price -> size
best_bid = None
best_ask = None

# --- CONFIG: thresholds ---
IMB_FLOW_THRESH   = 0.20   # orderflow imbalance
IMB_DEPTH_THRESH  = 0.20   # depth imbalance

RANGE_LOOKBACK = 6    
BREAKOUT_BUFFER = 0.0015  
PRODUCT = "XRP-USD"
STOP_MULTIPLIER = 0.8  

########################################
# UNIVERSAL SAFE WRAPPER
########################################
def safe_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"[BH ERROR] {func.__name__} failed: {e}")
        return {"error": str(e)}



########################################
# AUTHENTICATION
########################################
def create_client() -> RESTClient:
    api_key = Key.api_key
    api_secret = Key.api_secret
    if not api_key or not api_secret:
        raise RuntimeError("Missing COINBASE_API_KEY or COINBASE_API_SECRET")
    return RESTClient(api_key=api_key, api_secret=api_secret)

def startMarketDataFlow():
    global xrp_imbalance, xrp_buy_vol, xrp_sell_vol
    global imbalance_window, prev_imbalance, imbalance_delta
    global order_book_bids, order_book_asks, best_bid, best_ask

    while True:
        try:
            print("[WS] Starting market data flow...")

            # -------------------------
            # ON OPEN: SUBSCRIBE
            # -------------------------
            def on_open(ws):
                sub_trades = {
                    "type": "subscribe",
                    "channel": "market_trades",
                    "product_ids": ["XRP-USD"]
                }
                ws.send(json.dumps(sub_trades))

                sub_l2 = {
                    "type": "subscribe",
                    "channel": "level2",
                    "product_ids": ["XRP-USD"]
                }
                ws.send(json.dumps(sub_l2))

                print("[WS] Subscribed to XRP trades + level2.")

            # -------------------------
            # HANDLE TRADES
            # -------------------------
            def handle_market_trades(msg):
                global xrp_imbalance, xrp_buy_vol, xrp_sell_vol, flow_imb
                global imbalance_window, prev_imbalance, imbalance_delta

                events = msg.get("events", [])
                buy_vol = 0.0
                sell_vol = 0.0

                for event in events:
                    trades = event.get("trades", [])
                    for t in trades:
                        size = float(t["size"])
                        side = t["side"]
                        if side == "BUY":
                            buy_vol += size
                        elif side == "SELL":
                            sell_vol += size

                # accumulate per-message volume into the 1-minute totals
                xrp_buy_vol += buy_vol
                xrp_sell_vol += sell_vol

                # NEW: correct flow imbalance formula using the global accumulators
                total_flow = xrp_buy_vol + xrp_sell_vol
                flow_imb = (xrp_buy_vol - xrp_sell_vol) / total_flow if total_flow > 0 else 0.0

                # push flow imbalance into smoothing window
                imbalance_window.append(flow_imb)

                if len(imbalance_window) > 200:
                    imbalance_window.pop(0)

                # smoothed flow imbalance
                xrp_imbalance = sum(imbalance_window) / len(imbalance_window)

                # delta tracking
                imbalance_delta = xrp_imbalance - prev_imbalance
                prev_imbalance = xrp_imbalance


            # -------------------------
            # HANDLE LEVEL 2 ORDER BOOK
            # -------------------------
            def handle_level2(msg):
                global order_book_bids, order_book_asks, best_bid, best_ask

                now = time.time()
                STALE = 5  # seconds before a level is considered stale

                events = msg.get("events", [])
                for event in events:
                    updates = event.get("updates", [])
                    for u in updates:

                        side = u["side"]              # "bid" or "offer"
                        price = float(u["price_level"])
                        size = float(u["new_quantity"])

                        if side == "bid":
                            if size == 0.0:
                                order_book_bids.pop(price, None)
                            else:
                                order_book_bids[price] = (size, now)

                        elif side == "offer":
                            if size == 0.0:
                                order_book_asks.pop(price, None)
                            else:
                                order_book_asks[price] = (size, now)

                # -------------------------
                # PRUNE STALE LEVELS
                # -------------------------
                for price, (size, ts) in list(order_book_bids.items()):
                    if now - ts > STALE:
                        order_book_bids.pop(price, None)

                for price, (size, ts) in list(order_book_asks.items()):
                    if now - ts > STALE:
                        order_book_asks.pop(price, None)

                # -------------------------
                # BEST BID / BEST ASK
                # -------------------------
                best_bid = max(order_book_bids.keys()) if order_book_bids else None
                best_ask = min(order_book_asks.keys()) if order_book_asks else None

            # -------------------------
            # ON MESSAGE
            # -------------------------
            def on_message(ws, message):
                global s_data
                s_data = message

                try:
                    msg = json.loads(message)
                    channel = msg.get("channel")

                    if channel == "market_trades":
                        handle_market_trades(msg)

                    elif channel == "l2_data":
                        handle_level2(msg)

                    else:
                        pass

                except Exception as e:
                    print("[WS PARSE ERROR]", e)

            def on_error(ws, error):
                print("[WS ERROR]", error)

            def on_close(ws, code, msg):
                print("[WS CLOSED]", code, msg)

            ws = websocket.WebSocketApp(
                "wss://advanced-trade-ws.coinbase.com/l2_data",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever()

        except Exception as e:
            print("[WS FATAL ERROR] Market data crashed:", e)

        print("[WS] Restarting market data in 3 seconds...")
        time.sleep(3)


########################################
# Compute Depth Imbalance
########################################

def compute_depth_imbalance(n_levels=10):
    global order_book_bids, order_book_asks

    if not order_book_bids or not order_book_asks:
        return 0.0

    bid_prices = sorted(order_book_bids.keys(), reverse=True)[:n_levels]
    ask_prices = sorted(order_book_asks.keys())[:n_levels]

    bid_depth = sum(order_book_bids[p][0] for p in bid_prices)
    ask_depth = sum(order_book_asks[p][0] for p in ask_prices)


    depth = (bid_depth - ask_depth) / (bid_depth + ask_depth)
    depthScore = int((depth + 1) * 50)

    return depth, depthScore




def who_is_winning():
    """
    Returns: "BUY", "SELL", or "NEUTRAL"
    based on orderflow + depth.
    """
    global xrp_imbalance

    depth_imb = compute_depth_imbalance()

    flow_imb  = xrp_imbalance

    # BUY side winning: both flow and depth > threshold
    if flow_imb > IMB_FLOW_THRESH and depth_imb > IMB_DEPTH_THRESH:
        return "BUY"

    # SELL side winning: both flow and depth < -threshold
    if flow_imb < -IMB_FLOW_THRESH and depth_imb < -IMB_DEPTH_THRESH:
        return "SELL"

    # otherwise: no clear winner
    return "NEUTRAL"
















########################################
# ACCOUNT DETAILS
########################################
def getAccountBalance(client):
    resp = safe_call(client.get_accounts)
    if isinstance(resp, dict) and "error" in resp:
        return resp

    usd_value = 0
    for acct in resp["accounts"]:
        if acct["currency"] == "USD":
            usd_value = float(acct["available_balance"]["value"])
    return usd_value



def getOrderBook(client):
    resp = safe_call(client.list_orders, order_status="OPEN")
    return resp



def list_portfolios(client):
    resp = safe_call(client.get_portfolios)
    return resp



def list_accounts(client):
    resp = safe_call(client.get_accounts)
    return resp



def getRetailBalance(client, crypto_name):
    resp = safe_call(client.get_accounts)
    if isinstance(resp, dict) and "error" in resp:
        return resp

    for acct in resp["accounts"]:
        if acct["currency"] == crypto_name:
            return float(acct["available_balance"]["value"])
    return 0.0



########################################
# PLACE LIMIT ORDERS
########################################
def place_limit_order(client: RESTClient, side: str, product_id: str, base_size: str, limit_price: str):

    def _place():
        return client.create_order(
            client_order_id=str(uuid.uuid4()),
            product_id=product_id,
            side=side.upper(),
            order_configuration={
                "limit_limit_gtc": {
                    "base_size": base_size,
                    "limit_price": limit_price,
                    "post_only": True
                }
            }
        )

    resp = safe_call(_place)
    if isinstance(resp, dict) and "error" in resp:
        return resp
    return resp.to_dict()


########################################
# PLACE MARKET ORDERs
########################################
def place_market_order(client: RESTClient, side: str, product_id: str, base_size: str):
    """
    Places a market order on Coinbase Advanced Trade.
    base_size: crypto amount (e.g., "0.002")
    """

    def _place():
        return client.create_order(
            client_order_id=str(uuid.uuid4()),
            product_id=product_id,
            side=side.upper(),
            order_configuration={
                "market_market_ioc": {
                    "base_size": base_size
                }
            }
        )

    resp = safe_call(_place)
    if isinstance(resp, dict) and "error" in resp:
        return resp
    return resp.to_dict()


   


########################################
# CANCEL ORDERS
########################################
def cancel_order(client, order_id):
    resp = safe_call(client.cancel_order, order_id=order_id)
    return resp



def get_best_bid_ask(client, product_id="XRP-USD"):
    resp = client.get_product_book(product_id=product_id, limit=1)

    # Direct extraction — no wrappers, no safety, no get()
    best_bid = float(resp["pricebook"]["bids"][0]["price"])
    best_ask = float(resp["pricebook"]["asks"][0]["price"])

    return best_bid, best_ask



def get_candles(granularity_seconds, length):
    length_seconds = int(length * 3600)
    server_time = get_server_time()

    now = int(server_time)
    start = now - length_seconds
    end = now

    url = (
        f"https://api.exchange.coinbase.com/products/XRP-USD/candles"
        f"?granularity={granularity_seconds}&start={start}&end={end}"
    )

    #print(url, now)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, headers=headers, timeout=5)

        # Prevent printing HTML mess by checking content type
        if "application/json" not in r.headers.get("Content-Type", ""):
            time.sleep(2)
            return get_candles(granularity_seconds, length)

        try:
            data = r.json()
        except Exception:
            return []
        return list(reversed(data))

    except (ConnectionError, ReadTimeout, urllib3.exceptions.NewConnectionError, OSError) as e:
        print("REST timeout or unreachable host, retrying...", e)
        time.sleep(30)
        return get_candles(granularity_seconds, length)
    

import json
import time

def get_server_time():
    try:
        r = requests.get("https://api.exchange.coinbase.com/time", timeout=5)
        raw = r.text
        raw = raw.replace('.}', '}')
        data = json.loads(raw)
        epoch = float(data.get("epoch", 0))
        if epoch <= 0:
            raise ValueError("Invalid epoch")
        return int(epoch)
    except Exception:
        # Fallback so your bot never gets a zero timestamp
        return int(time.time())




def get_last_minute_close(length_minutes=120):
    """
    Returns the last close from 1-minute candles.
    length_minutes controls how far back to fetch.
    """

    granularity_seconds = 60
    length_seconds = int(length_minutes * 60)

    # -----------------------------
    # SAFE SERVER TIME FETCH
    # -----------------------------
    try:
        r_time = requests.get(
            "https://api.exchange.coinbase.com/time",
            timeout=5
        )

        # Ensure JSON content
        if "application/json" not in r_time.headers.get("Content-Type", ""):
            print("NON‑JSON server time response:", r_time.text[:200])
            time.sleep(2)
            return 0

        try:
            data_time = r_time.json()
        except json.JSONDecodeError:
            print("Malformed server time JSON:", r_time.text[:200])
            time.sleep(2)
            return 0

        if "epoch" not in data_time:
            print("Missing 'epoch' in server time:", data_time)
            time.sleep(2)
            return 0

        server_time = int(data_time["epoch"])

    except Exception as e:
        print("Server time fetch failed:", e)
        time.sleep(2)
        return 0

    # -----------------------------
    # Candle fetch
    # -----------------------------
    start = server_time - length_seconds
    end = server_time

    url = (
        f"https://api.exchange.coinbase.com/products/XRP-USD/candles"
        f"?granularity={granularity_seconds}&start={start}&end={end}"
    )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, headers=headers, timeout=5)

        # Avoid HTML garbage
        if "application/json" not in r.headers.get("Content-Type", ""):
            print("NON‑JSON candle response:", r.text[:200])
            time.sleep(2)
            return 0

        try:
            data = r.json()
        except json.JSONDecodeError:
            print("Malformed candle JSON:", r.text[:200])
            time.sleep(2)
            return 0

        # Coinbase returns newest → oldest, so reverse
        candles = list(reversed(data))

        # Candle format:
        # [timestamp, low, high, open, close, volume]
        last_close = candles[-1][4]
        return last_close

    except (ConnectionError, ReadTimeout, urllib3.exceptions.NewConnectionError, OSError) as e:
        print("Network error:", e)
        time.sleep(30)
        return 0




def _to_df(candles):
    df = s.pd.DataFrame(candles, columns=['time','open','high','low','close','volume'])
    df.time = s.pd.to_datetime(df.time, unit='s', utc=True)
    df = df.set_index('time').astype(float)
    return df[['open','high','low','close','volume']]



def get_15m_atr():
    candles = get_candles(900)  # 15m candles
    return atr(candles)         # your ATR function

def get_1h_atr():
    candles = get_candles(3600)
    return atr(candles)

def atr(candles):
    trs = []
    for i in range(1, len(candles)):
        high = float(candles[i][2])
        low = float(candles[i][1])
        prev_close = float(candles[i-1][4])

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        trs.append(tr)

    return sum(trs) / len(trs) if trs else 0.0



def compute_limit_price(client, side):
    """
    Returns a jittered MAKER-SAFE limit price for BUY or SELL.
    Ensures the order will NOT fill immediately (post-only compatible).
    """

    # Tiny jitter to avoid predictable pricing
    jitter = random.uniform(0.99998742, 1.000172)

    if side == "BUY":
        best_bid, best_ask = get_best_bid_ask(client, "XRP-USD")

        # Tiny jitter: ±0.01%
        jitter = random.uniform(0.9999, 0.9995)

        # Maker BUY must be AT or just below best bid
        return round(best_bid * jitter, 2)
    else:  # SELL
        best_bid, best_ask = get_best_bid_ask(client, "XRP-USD")
        # Tiny jitter: ±0.01%
        jitter = random.uniform(1.0000, 1.00015)

        # Maker SELL must be AT or just above best ask
        return round(best_ask * jitter, 2)


def calculate_stop_loss(entry_price, risk_percent):
    """
    Simple stop loss calculator.
    entry_price: float - the price you entered the trade
    risk_percent: float - percent risk as decimal (0.01 = 1%)
    """
    if entry_price is None or entry_price <= 0:
        return None

    if risk_percent < 0 or risk_percent > 0.20:
        # Safety guard: prevents accidental oversized risk
        risk_percent = max(0, min(risk_percent, 0.20))

    stop_loss_price = entry_price * (1 - risk_percent)
    return round(stop_loss_price, 2)


def save_last_buy_price(price, filename="last_buy_price.txt"):
    try:
        with open(filename, "w") as f:
            f.write(str(price))
    except Exception as e:
        print("⚠️ Could not save last buy price:", e)


def load_last_buy_price(filename="last_buy_price.txt"):
    try:
        with open(filename, "r") as f:
            return float(f.read().strip())
    except FileNotFoundError:
        return None  # no previous buy
    except Exception as e:
        print("⚠️ Could not read last buy price:", e)
        return None
    

def position_size(account_usd: float,
                  mid_price: float,
                  risk_pct: float,
                  confidence: float):
    """
    Small, intelligent position sizing formula.
    - account_usd: total USD account value
    - mid_price: current mid price (bid+ask)/2
    - risk_pct: fraction of account you're willing to risk (e.g. 0.02)
    - confidence: 0..1 (trend strength, signal confidence, etc.)
    """

    confidence = max(0.0, min(confidence, 1.0))  # clamp

    # Risk capital scaled by confidence (50% to 100%)
    risk_usd = account_usd * risk_pct * (0.5 + 0.5 * confidence)

    # Convert USD risk into crypto size
    size = risk_usd / mid_price
    return size


def micro_volatility(candles):
    # candles: list of OHLCV arrays
    # volatility window should be > trend window (e.g., 300s × 6 = 30 minutes)
    closes = [c[4] for c in candles]

    if len(closes) < 3:
        return 0.0

    # compute log returns for stability
    returns = np.diff(np.log(closes))

    # standard deviation of returns
    vol = np.std(returns)

    return float(vol)



def compute_volume_ratio(candles):
    """
    candles: list of lists in format:
    [timestamp, open, high, low, close, volume]
    """

    # Current candle volume
    current_volume = float(candles[-1][5])

    # Last 20 candle volumes
    last_20_volumes = [float(c[5]) for c in candles[-21:-1]]

    # Avoid division errors
    avg_volume = sum(last_20_volumes) / len(last_20_volumes)
    if avg_volume <= 0:
        return 0

    return current_volume / avg_volume


#Add a bh function that can open a GUI display window so we can print in gui if we need.


from datetime import datetime, timezone

def parse_open_order(orderbook_resp):
    """
    Parses a Coinbase Advanced Trade ListOrdersResponse object.
    Returns:
      has_order (bool)
      order_id (str or None)
      age_seconds (float or None)
      fee_percent (float or None)
      product (str or None)
      side (str or None)
    """

    # Coinbase returns an object with .orders, not a dict
    orders = getattr(orderbook_resp, "orders", None)

    # 1. No orders at all
    if not orders:
        return False, None, None, None, None, None

    # 2. Use the first order (you only allow one open order)
    o = orders[0]

    # Extract fields
    order_id = o.order_id
    product = o.product_id
    side = o.side

    # 3. Age in seconds
    created_str = o.created_time
    try:
        created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        now_dt = datetime.now(timezone.utc)
        age_seconds = (now_dt - created_dt).total_seconds()
    except Exception:
        age_seconds = None

    # 4. Fee percent
    try:
        total_fees = float(o.total_fees or 0)
        total_value = float(o.total_value_after_fees or 0)
        fee_percent = (total_fees / total_value * 100) if total_value > 0 else 0.0
    except Exception:
        fee_percent = None

    return True, order_id, age_seconds, fee_percent, product, side
