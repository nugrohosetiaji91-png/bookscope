#!/usr/bin/env python3
"""
bookscope - High-fidelity orderbook recorder for Polymarket prediction markets
===============================================================================
Records every orderbook update on rolling BTC 5-minute up/down markets for
market-microstructure research (market-maker behavior, liquidity dynamics).

Captures:
  1. Every book update (bid/ask prices + sizes, top-N depth)
  2. Order Book Imbalance (OBI) on every update
  3. Spread changes
  4. Liquidity events (pulls, adds, new walls)
  5. Session metadata (market window boundaries)

Usage:
  python3 recorder.py                # record indefinitely
  python3 recorder.py --duration 6   # record for 6 hours

Output: ./poly_data/<date>/  (gzipped JSONL, hourly rotation)
"""

import json
import time
import os
import gzip
import threading
import argparse
import signal
import sys
import requests
from datetime import datetime, timezone
from collections import defaultdict

try:
    import websocket
except ImportError:
    print("pip install websocket-client")
    sys.exit(1)

# --- CONFIG ------------------------------------------------------
POLY_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API      = "https://gamma-api.polymarket.com/events"
PERIOD         = 300          # 5-minute windows

DEPTH_LEVELS   = 10          # record top 10 levels
FLUSH_INTERVAL = 5           # flush to disk every 5 seconds
STATS_INTERVAL = 30          # print stats every 30 seconds
OBI_DEPTH      = 5           # OBI calculation depth

# --- GLOBALS -----------------------------------------------------
state = {
    "yes_book": {"bids": {}, "asks": {}},
    "no_book":  {"bids": {}, "asks": {}},
    "yes_token": None,
    "no_token": None,
    "market_id": None,
    "session_end": 0,
    "ws_connected": False,
    "current_slug": None,
}
stats = defaultdict(int)
lock = threading.Lock()
running = True
writers = {}

# --- FILE WRITER -------------------------------------------------
class StreamWriter:
    def __init__(self, filepath):
        self.f = gzip.open(filepath + ".gz", "at", encoding="utf-8")
        self.lock = threading.Lock()
        self.count = 0

    def write(self, record):
        line = json.dumps(record)
        with self.lock:
            self.f.write(line + "\n")
            self.count += 1

    def flush(self):
        with self.lock:
            self.f.flush()

    def close(self):
        with self.lock:
            self.f.close()

def get_writer(stream_name):
    if stream_name not in writers:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        hour = datetime.now(timezone.utc).strftime("%H")
        path = f"./poly_data/{date_str}"
        os.makedirs(path, exist_ok=True)
        filepath = f"{path}/{stream_name}_{hour}.jsonl"
        writers[stream_name] = StreamWriter(filepath)
    return writers[stream_name]

# --- MARKET DISCOVERY -----------------------
def current_window_ts():
    return int(time.time()) - (int(time.time()) % PERIOD)

def current_slug():
    return f"btc-updown-5m-{current_window_ts()}"

def fetch_market(slug):
    """Fetch market data from gamma-api."""
    try:
        r = requests.get(GAMMA_API, params={"slug": slug}, timeout=8)
        data = r.json()
        if not data:
            return None, None, None
        market = data[0]["markets"][0]
        tokens = json.loads(market["clobTokenIds"])
        end_iso = (market.get("endDate") or market.get("end_date_iso")
                   or market.get("endDateIso"))
        if end_iso:
            try:
                end_ts = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
            except:
                end_ts = current_window_ts() + PERIOD
        else:
            end_ts = current_window_ts() + PERIOD

        yes_token = tokens[0]
        no_token = tokens[1]
        return yes_token, no_token, end_ts
    except Exception as e:
        print(f"[ERR] fetch_market: {e}")
        return None, None, None

# --- ORDERBOOK PROCESSING ---------------------------------------

def calc_obi(book, depth=OBI_DEPTH):
    """Calculate Order Book Imbalance"""
    bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:depth]
    asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:depth]

    bid_vol = sum(float(s) for _, s in bids)
    ask_vol = sum(float(s) for _, s in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total

def get_book_snapshot(book, levels=DEPTH_LEVELS):
    """Get top N levels of orderbook"""
    bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:levels]
    asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:levels]
    return {
        "bids": [[float(p), float(s)] for p, s in bids],
        "asks": [[float(p), float(s)] for p, s in asks],
    }

def get_best(book):
    """Get best bid/ask"""
    bids = book["bids"]
    asks = book["asks"]
    best_bid = max((float(p) for p in bids if float(bids[p]) > 0), default=0)
    best_ask = min((float(p) for p in asks if float(asks[p]) > 0), default=1)
    return best_bid, best_ask

def detect_liquidity_event(prev_snap, curr_snap, side):
    """
    Detect significant liquidity changes:
    - PULL: volume drops > 50% at a level
    - ADD:  volume increases > 100% at a level (new wall)
    - SWEEP: multiple levels consumed
    """
    events = []
    if prev_snap is None:
        return events

    prev_levels = {p: s for p, s in prev_snap.get(side, [])}
    curr_levels = {p: s for p, s in curr_snap.get(side, [])}

    for price, prev_size in prev_levels.items():
        curr_size = curr_levels.get(price, 0)
        if prev_size > 0:
            change_pct = (curr_size - prev_size) / prev_size

            if change_pct <= -0.5 and prev_size >= 5:  # >50% drop, min 5 contracts
                events.append({
                    "type": "PULL",
                    "side": side,
                    "price": price,
                    "from": prev_size,
                    "to": curr_size,
                    "pct": change_pct,
                })
            elif change_pct >= 1.0 and curr_size >= 10:  # >100% increase, min 10
                events.append({
                    "type": "ADD",
                    "side": side,
                    "price": price,
                    "from": prev_size,
                    "to": curr_size,
                    "pct": change_pct,
                })

    # New levels that didn't exist before (wall placement)
    for price, curr_size in curr_levels.items():
        if price not in prev_levels and curr_size >= 10:
            events.append({
                "type": "NEW_WALL",
                "side": side,
                "price": price,
                "size": curr_size,
            })

    return events

# --- WEBSOCKET HANDLER ------------------------------------------

prev_yes_snap = None
prev_no_snap = None

def _safe_float(val, default=0.0):
    """Safe float conversion."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _handle_book_snapshot(asset_id, bids_raw, asks_raw):
    """Full snapshot - replace local book."""
    with lock:
        if asset_id == state.get("yes_token"):
            book_key = "yes_book"
        elif asset_id == state.get("no_token"):
            book_key = "no_book"
        else:
            return None

    b_dict = {}
    a_dict = {}
    for b in (bids_raw or []):
        if not isinstance(b, dict): continue
        price = b.get("price")
        size  = _safe_float(b.get("size"), 0.0)
        if price is None or size <= 0: continue
        b_dict[str(price)] = size
    for a in (asks_raw or []):
        if not isinstance(a, dict): continue
        price = a.get("price")
        size  = _safe_float(a.get("size"), 0.0)
        if price is None or size <= 0: continue
        a_dict[str(price)] = size

    with lock:
        state[book_key]["bids"] = b_dict
        state[book_key]["asks"] = a_dict

    return book_key

def _handle_price_change(asset_id, changes):
    """Incremental update - apply to local book."""
    with lock:
        if asset_id == state.get("yes_token"):
            book_key = "yes_book"
        elif asset_id == state.get("no_token"):
            book_key = "no_book"
        else:
            return None

    with lock:
        for change in (changes or []):
            if not isinstance(change, dict): continue
            price = change.get("price")
            side  = str(change.get("side", "")).upper()
            size  = _safe_float(change.get("size"), 0.0)
            if price is None: continue
            side_key = "bids" if side in ("BUY", "BID") else \
                       ("asks" if side in ("SELL", "ASK") else None)
            if side_key is None: continue
            if size <= 0:
                state[book_key][side_key].pop(str(price), None)
            else:
                state[book_key][side_key][str(price)] = size

    return book_key

def _process_event(item):
    """Process single WS event."""
    if not isinstance(item, dict): return
    event_type = item.get("event_type")
    asset_id   = item.get("asset_id")
    if not asset_id: return

    if event_type == "book":
        bids = item.get("bids") or item.get("buys") or []
        asks = item.get("asks") or item.get("sells") or []
        book_key = _handle_book_snapshot(asset_id, bids, asks)
    elif event_type == "price_change":
        changes = item.get("changes") or []
        book_key = _handle_price_change(asset_id, changes)
    else:
        return  # tick_size_change, last_trade_price - skip

    if book_key:
        _record_book_state(asset_id, book_key, event_type)

def process_book_update(data):
    """Route WS message to _process_event."""
    _process_event(data)

def _record_book_state(asset_id, book_key, event_type):
    """Record book state after update - recorder-specific logic"""
    global prev_yes_snap, prev_no_snap

    ts = int(time.time() * 1000)

    with lock:
        if asset_id == state.get("yes_token"):
            side_label = "YES"
        else:
            side_label = "NO"
        book = state[book_key]

    # Calculate metrics
    with lock:
        obi = calc_obi(book, OBI_DEPTH)
        snap = get_book_snapshot(book)
        best_bid, best_ask = get_best(book)
        spread = best_ask - best_bid

        bid_vol = sum(s for _, s in snap["bids"])
        ask_vol = sum(s for _, s in snap["asks"])

    # Detect liquidity events
    prev_snap = prev_yes_snap if side_label == "YES" else prev_no_snap
    liq_events = []
    liq_events += detect_liquidity_event(prev_snap, snap, "bids")
    liq_events += detect_liquidity_event(prev_snap, snap, "asks")

    if side_label == "YES":
        prev_yes_snap = snap
    else:
        prev_no_snap = snap

    # --- RECORD: book_update --------------------------------
    record = {
        "ts": ts,
        "side": side_label,
        "obi": round(obi, 4),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": round(spread, 4),
        "bid_vol": round(bid_vol, 1),
        "ask_vol": round(ask_vol, 1),
        "book": snap,
    }
    get_writer("book_update").write(record)
    stats["book_updates"] += 1

    # --- RECORD: liquidity events ---------------------------
    if liq_events:
        for ev in liq_events:
            ev["ts"] = ts
            ev["token_side"] = side_label
            ev["obi_at_event"] = round(obi, 4)
            get_writer("liq_events").write(ev)
            stats["liq_events"] += 1

    # --- RECORD: OBI timeseries (compact, for plotting) -----
    obi_record = {
        "ts": ts,
        "s": side_label,
        "obi": round(obi, 4),
        "bb": best_bid,
        "ba": best_ask,
        "bv": round(bid_vol, 1),
        "av": round(ask_vol, 1),
    }
    get_writer("obi_ts").write(obi_record)
    stats["obi_records"] += 1

def on_ws_message(ws, message):
    try:
        msgs = json.loads(message)
        if isinstance(msgs, list):
            for m in msgs:
                process_book_update(m)
        elif isinstance(msgs, dict):
            process_book_update(msgs)
    except Exception as e:
        stats["errors"] += 1

def on_ws_open(ws):
    with lock:
        yes_token = state["yes_token"]
        no_token = state["no_token"]

    assets = []
    if yes_token:
        assets.append(yes_token)
    if no_token:
        assets.append(no_token)

    if assets:
        sub = {"type": "MARKET", "assets_ids": assets}
        ws.send(json.dumps(sub))
        print(f"[WS] Connected - subscribed to {len(assets)} assets")
        with lock:
            state["ws_connected"] = True
        stats["connects"] += 1

def on_ws_close(ws, code, msg):
    print(f"[WS] Closed: {code}")
    with lock:
        state["ws_connected"] = False
    if running:
        time.sleep(3)
        start_ws()

def on_ws_error(ws, error):
    print(f"[WS] Error: {error}")
    stats["ws_errors"] += 1

def start_ws():
    ws = websocket.WebSocketApp(
        POLY_WS_URL,
        on_message=on_ws_message,
        on_open=on_ws_open,
        on_close=on_ws_close,
        on_error=on_ws_error,
    )
    t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20, "ping_timeout": 10}, daemon=True)
    t.start()
    return ws

# --- SESSION TRACKING --------------------------------------------

def record_session_boundary(event_type, market):
    """Record session start/end for correlation"""
    record = {
        "ts": int(time.time() * 1000),
        "event": event_type,
        "market_id": market.get("condition_id", ""),
        "question": market.get("question", ""),
        "end_date": market.get("end_date_iso", ""),
    }
    get_writer("sessions").write(record)
    stats["sessions"] += 1

# --- DASHBOARD ---------------------------------------------------

def print_dashboard(start_time):
    elapsed = time.time() - start_time
    hours = elapsed / 3600

    with lock:
        yes_obi = calc_obi(state["yes_book"])
        no_obi = calc_obi(state["no_book"])
        yes_bid, yes_ask = get_best(state["yes_book"])
        no_bid, no_ask = get_best(state["no_book"])
        connected = state["ws_connected"]

    # Disk size
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = f"./poly_data/{date_str}"
    total_size = 0
    if os.path.exists(path):
        for f in os.listdir(path):
            fp = os.path.join(path, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    size_mb = total_size / (1024 * 1024)

    ws_status = "\033[92mCONNECTED\033[0m" if connected else "\033[91mDISCONNECTED\033[0m"

    print(f"\n{'='*55}")
    print(f"  POLYMARKET RECORDER - {hours:.1f}h - {ws_status}")
    print(f"{'='*55}")
    print(f"  OBI YES: {yes_obi:+.2f}    OBI NO: {no_obi:+.2f}")
    print(f"  YES bid={yes_bid:.2f} ask={yes_ask:.2f}")
    print(f"  NO  bid={no_bid:.2f} ask={no_ask:.2f}")
    print(f"{'-'*55}")
    print(f"  book_updates : {stats['book_updates']:>10,}")
    print(f"  obi_records  : {stats['obi_records']:>10,}")
    print(f"  liq_events   : {stats['liq_events']:>10,}")
    print(f"  sessions     : {stats['sessions']:>10,}")
    print(f"  errors       : {stats['errors']:>10,}")
    print(f"  disk         : {size_mb:>10.1f} MB")
    if hours > 0.01:
        total = stats['book_updates'] + stats['obi_records']
        print(f"  rate         : {total/elapsed:>10.0f} evt/sec")
        print(f"  est 24h      : {size_mb/hours*24:>10.0f} MB")
    print(f"{'='*55}\n")

# --- MAIN --------------------------------------------------------

def main():
    global running

    parser = argparse.ArgumentParser(description="Polymarket BTC 5-min Orderbook Recorder")
    parser.add_argument("--duration", type=float, default=0, help="Recording hours (0=indefinite)")
    args = parser.parse_args()

    duration_sec = args.duration * 3600 if args.duration > 0 else float("inf")

    print(f"""
+======================================================+
|   POLYMARKET BTC 5-MIN ORDERBOOK RECORDER            |
|                                                      |
|   Streams:                                           |
|     * book_update  - every orderbook change          |
|     * obi_ts       - OBI timeseries                  |
|     * liq_events   - liquidity pull/add/wall events  |
|     * sessions     - market session boundaries       |
|                                                      |
|   Output: ./poly_data/<date>/                        |
+======================================================+
""")

    # Signal handler
    def signal_handler(sig, frame):
        global running
        print("\n[STOP] Shutting down...")
        running = False
    signal.signal(signal.SIGINT, signal_handler)

    start_time = time.time()
    last_stats = time.time()
    last_flush = time.time()
    last_market_check = 0

    print("[INIT] Looking for active BTC 5-min market...")

    while running:
        now = time.time()

        # Check duration
        if now - start_time >= duration_sec:
            print(f"\n[DONE] Duration reached. Stopping.")
            break

        # Find/refresh market every 5 seconds
        if now - last_market_check >= 5:
            slug = current_slug()

            if slug != state.get("current_slug"):
                # New 5-minute window
                yes_token, no_token, end_ts = fetch_market(slug)

                if yes_token and no_token:
                    with lock:
                        old_yes = state["yes_token"]
                        state["yes_token"] = yes_token
                        state["no_token"] = no_token
                        state["market_id"] = slug
                        state["session_end"] = end_ts
                        state["current_slug"] = slug

                    # New market detected - reconnect WS
                    if yes_token != old_yes:
                        print(f"[MARKET] New session: {slug}")
                        record_session_boundary("NEW_SESSION", {
                            "condition_id": slug,
                            "question": f"BTC Up/Down 5m - {slug}",
                            "end_date": "",
                        })
                        # Clear books
                        with lock:
                            state["yes_book"] = {"bids": {}, "asks": {}}
                            state["no_book"] = {"bids": {}, "asks": {}}
                        start_ws()
                else:
                    if stats["sessions"] == 0:
                        print(f"[WAIT] Market not found for {slug}, retrying...")
            
            last_market_check = now

        # Flush
        if now - last_flush >= FLUSH_INTERVAL:
            for w in writers.values():
                try:
                    w.flush()
                except:
                    pass
            last_flush = now

        # Stats
        if now - last_stats >= STATS_INTERVAL:
            print_dashboard(start_time)
            last_stats = now

        # Rotate files hourly
        # (simple: just let new hour create new writers)

        time.sleep(1)

    # Cleanup
    running = False
    print_dashboard(start_time)
    for w in writers.values():
        try:
            w.flush()
            w.close()
        except:
            pass
    print("[DONE] All data saved.")

if __name__ == "__main__":
    main()