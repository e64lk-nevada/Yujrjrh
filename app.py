import math, threading, time, requests
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)
prices = deque(maxlen=1800)  # timestamp, price
signals = deque(maxlen=100)
lock = threading.Lock()
WINDOW = 300
BINANCE_URL = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
# Chainlink BTC/USD Mainnet Data Feed (reference spot feed; NOT Polymarket's TWAP60 stream)
CHAINLINK_RPC = 'https://ethereum-rpc.publicnode.com'
CHAINLINK_FEED = '0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c'
CHAINLINK_SELECTOR = '0xfeaf968c'  # latestRoundData()

state = {'price': None, 'updated': None, 'window_open': None, 'price_to_beat': None,
         'window_seconds': WINDOW, 'seconds_passed': 0, 'signal': None, 'status': 'Starting price feed…',
         'chainlink_price': None, 'chainlink_updated': None, 'chainlink_age': None,
         'chainlink_status': 'Waiting for oracle read', 'last_error': None}


def fetch_price():
    r = requests.get(BINANCE_URL, timeout=8)
    r.raise_for_status()
    return float(r.json()['price'])


def fetch_chainlink_reference():
    # Free public RPC read of Chainlink's standard BTC/USD reference feed.
    # This is not the Polymarket BTC 5m settlement TWAP60 data stream.
    payload = {'jsonrpc':'2.0','id':1,'method':'eth_call',
              'params':[{'to':CHAINLINK_FEED,'data':CHAINLINK_SELECTOR},'latest']}
    r = requests.post(CHAINLINK_RPC, json=payload, timeout=12)
    r.raise_for_status()
    data = r.json()
    if 'error' in data or not data.get('result'):
        raise RuntimeError('Chainlink RPC returned no result')
    raw = data['result'][2:]
    words = [raw[i:i+64] for i in range(0, len(raw), 64)]
    if len(words) < 4:
        raise RuntimeError('Invalid Chainlink round data')
    answer = int(words[1], 16)
    if answer >= 2**255:
        answer -= 2**256
    updated_at = int(words[3], 16)
    if answer <= 0 or updated_at <= 0:
        raise RuntimeError('Invalid Chainlink price/timestamp')
    return answer / 1e8, updated_at


def price_at_or_before(seconds_ago):
    now = time.time()
    target = now - seconds_ago
    with lock:
        arr = list(prices)
    if not arr:
        return None
    # choose sample nearest target, but don't use one that is too far away
    p = min(arr, key=lambda x: abs(x[0] - target))
    if abs(p[0] - target) > max(4, seconds_ago * 0.35):
        return None
    return p[1]


def momentum_bps(seconds=12):
    p0 = price_at_or_before(seconds)
    with lock:
        p1 = prices[-1][1] if prices else None
    if not p0 or not p1 or p0 <= 0:
        return None
    return (p1 - p0) / p0 * 10000



def volatility_bps(lookback=60):
    now = time.time()
    with lock:
        arr = [(t, p) for t, p in prices if t >= now - lookback]
    if len(arr) < 10:
        return None
    # normalized per-second log returns; estimate standard deviation over 1 second
    returns = []
    for (t0, p0), (t1, p1) in zip(arr, arr[1:]):
        dt = t1 - t0
        if p0 > 0 and p1 > 0 and dt > 0:
            returns.append((math.log(p1 / p0) * 10000) / math.sqrt(dt))
    if len(returns) < 8:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
    return math.sqrt(variance)


def build_signal(open_price, current, passed):
    if passed < 180 or passed >= WINDOW:
        return None
    lead = (current - open_price) / open_price * 10000
    time_left = WINDOW - passed
    if abs(lead) < 3:
        return None
    side = 'UP' if lead > 0 else 'DOWN'
    m12 = momentum_bps(12)
    m5 = momentum_bps(5)
    if m12 is None or m5 is None:
        return None
    if side == 'UP' and not (m12 > 1.5 and m5 > 0):
        return None
    if side == 'DOWN' and not (m12 < -1.5 and m5 < 0):
        return None
    vol = volatility_bps(60)
    if vol is None or vol <= 0:
        return None
    # Volatility estimate scaled to remaining seconds; volatility is normalized per sqrt-second.
    expected_noise = vol * math.sqrt(time_left)
    if expected_noise <= 0:
        return None
    edge_ratio = abs(lead) / expected_noise
    if edge_ratio < 0.60:
        return None
    score = min(0.95, 0.60 + edge_ratio * 0.12)
    return {'side': side, 'score': round(score, 3), 'lead_bps': round(lead, 2),
            'momentum_12s': round(m12, 2), 'momentum_5s': round(m5, 2),
            'volatility': round(vol, 3), 'edge_ratio': round(edge_ratio, 3),
            'time_left': time_left, 'timestamp': datetime.now(timezone.utc).isoformat()}


def feed_loop():
    last_oracle_check = 0
    while True:
        try:
            p = fetch_price()
            now = time.time()
            if now - last_oracle_check >= 60:
                try:
                    oracle_price, oracle_ts = fetch_chainlink_reference()
                    with lock:
                        state['chainlink_price'] = oracle_price
                        state['chainlink_updated'] = datetime.fromtimestamp(oracle_ts, timezone.utc).strftime('%H:%M:%S UTC')
                        state['chainlink_age'] = max(0, int(now - oracle_ts))
                        state['chainlink_status'] = 'Reference feed updated' if now - oracle_ts < 7200 else 'Oracle reference is stale'
                except Exception as oracle_error:
                    with lock:
                        state['chainlink_status'] = 'Oracle read unavailable: ' + str(oracle_error)[:140]
                last_oracle_check = now
            now = time.time()
            with lock:
                prices.append((now, p))
                state['price'] = p
                state['updated'] = datetime.now().strftime('%H:%M:%S')
                state['last_error'] = None
                state['status'] = 'Live feed connected'
                # reset five-minute candle/window on wall-clock aligned boundaries
                win_start = int(now // WINDOW) * WINDOW
                if state['window_open'] != win_start:
                    state['window_open'] = win_start
                    state['price_to_beat'] = p
                    state['signal'] = None
                    state['seconds_passed'] = 0
                passed = int(now - state['window_open'])
                state['seconds_passed'] = passed
                open_price = state['price_to_beat']
            sig = build_signal(open_price, p, passed) if open_price else None
            with lock:
                if sig and state['signal'] is None:
                    state['signal'] = sig
                    signals.appendleft(sig)
        except Exception as e:
            with lock:
                state['last_error'] = str(e)
                state['status'] = 'Feed error — retrying'
        time.sleep(2)


@app.route('/')
def index():
    return render_template_string(PAGE)


@app.route('/api/state')
def api_state():
    with lock:
        s = dict(state)
        sigs = list(signals)
    s['signals'] = sigs
    return jsonify(s)


PAGE = r'''<!doctype html><html lang="uk"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#0b1220"><title>EdgeMarket v2 · BTC 5M</title>
<style>*{box-sizing:border-box}body{margin:0;background:#0b1220;color:#e8eef8;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:16px}.wrap{max-width:760px;margin:auto}.top{display:flex;justify-content:space-between;align-items:center;gap:10px}.muted{color:#92a2b8}.pill{border:1px solid #26364f;border-radius:30px;padding:6px 10px;font-size:12px}.card{background:#111b2b;border:1px solid #24344b;border-radius:18px;padding:16px;margin-top:14px}.price{font-size:clamp(32px,9vw,50px);font-weight:750;letter-spacing:-1px;margin:4px 0}.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.metric{background:#0d1625;border-radius:12px;padding:12px}.metric .v{font-size:22px;font-weight:700;margin-top:4px}.side{font-size:32px;font-weight:800}.up{color:#42d392}.down{color:#ff6b78}.neutral{color:#b8c4d6}.bar{height:8px;background:#253249;border-radius:99px;overflow:hidden;margin-top:10px}.fill{height:100%;background:#42d392;width:0}.small{font-size:13px}.row{display:flex;justify-content:space-between;align-items:center;gap:8px}.history{max-height:260px;overflow:auto}.signalrow{border-bottom:1px solid #24344b;padding:10px 0}.foot{font-size:12px;color:#8c9bb0;margin:14px 2px}.btn{border:1px solid #2c405d;border-radius:10px;background:#17253a;color:#dfe9f7;padding:10px 14px} @media(min-width:620px){body{padding:26px}.grid{grid-template-columns:repeat(4,1fr)}}</style></head><body><div class="wrap"><div class="top"><div><b>⚡ EdgeMarket v2</b><div class="muted small">BTC/USDT · 5-minute direction monitor</div></div><span id="status" class="pill">Connecting…</span></div>
<div class="card"><div class="muted small">BTC / USDT live price · Binance (signal feed)</div><div id="price" class="price">—</div><div class="row small muted"><span>Price to beat: <b id="open">—</b></span><span>Window: <b id="timer">—</b></span></div><div style="border-top:1px solid #24344b;margin-top:12px;padding-top:10px"><div class="muted small">Chainlink BTC/USD reference (not TWAP60 settlement)</div><div id="chainlink" style="font-weight:700;font-size:20px">—</div><div class="muted small" id="oracleStatus">Oracle waiting…</div></div></div>
<div class="card"><div class="muted small">Current window signal (checks from T+180s)</div><div id="side" class="side neutral">WAITING</div><div id="desc" class="muted small">Collecting market data…</div><div class="bar"><div id="fill" class="fill"></div></div><div class="row small muted" style="margin-top:8px"><span>Elapsed</span><span id="elapsed">—</span></div></div>
<div class="grid"><div class="card metric"><div class="muted small">Lead</div><div class="v" id="lead">—</div></div><div class="card metric"><div class="muted small">Momentum 12s</div><div class="v" id="m12">—</div></div><div class="card metric"><div class="muted small">Momentum 5s</div><div class="v" id="m5">—</div></div><div class="card metric"><div class="muted small">Edge ratio</div><div class="v" id="edge">—</div></div></div>
<div class="card"><div class="row"><b>Signal log</b><button class="btn" onclick="location.reload()">Refresh</button></div><div id="history" class="history muted small" style="margin-top:8px">No signals recorded yet.</div></div><div class="foot">Signals are experimental estimates, not financial advice. Render free web services may sleep when idle; dashboard activity may wake the service. Score is not a calibrated probability.</div></div>
<script>const fmt=(n,d=2)=>n==null?'—':Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});function render(s){document.getElementById('status').textContent=s.status||'Status';document.getElementById('price').textContent=s.price==null?'—':'$'+Number(s.price).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});document.getElementById('open').textContent=s.price_to_beat==null?'—':'$'+fmt(s.price_to_beat,2);document.getElementById('chainlink').textContent=s.chainlink_price==null?'—':'$'+fmt(s.chainlink_price,2);document.getElementById('oracleStatus').textContent=s.chainlink_status||'Oracle status unavailable'+(s.chainlink_updated?' · '+s.chainlink_updated:'')+(s.chainlink_age!=null?' · age '+s.chainlink_age+'s':'');let passed=s.seconds_passed||0, left=Math.max(0,300-passed);document.getElementById('timer').textContent=Math.floor(left/60)+':'+String(left%60).padStart(2,'0');document.getElementById('elapsed').textContent=passed+' / 300 s';document.getElementById('fill').style.width=Math.min(100,passed/3)+'%';let sig=s.signal,side=document.getElementById('side');if(sig){side.textContent=sig.side;side.className='side '+(sig.side==='UP'?'up':'down');document.getElementById('desc').textContent='Score '+Math.round(sig.score*100)+'% · '+sig.time_left+'s left · momentum confirmed';document.getElementById('lead').textContent=fmt(sig.lead_bps)+' bps';document.getElementById('m12').textContent=fmt(sig.momentum_12s)+' bps';document.getElementById('m5').textContent=fmt(sig.momentum_5s)+' bps';document.getElementById('edge').textContent=fmt(sig.edge_ratio,3);}else{side.textContent=passed<180?'WAIT · T+180':'NO TRADE';side.className='side neutral';document.getElementById('desc').textContent=s.last_error?'Feed issue: '+s.last_error:(passed<180?'Signal gate opens after 180 seconds.':'No setup passed filters yet.');document.getElementById('lead').textContent='—';document.getElementById('m12').textContent='—';document.getElementById('m5').textContent='—';document.getElementById('edge').textContent='—'}let list=s.signals||[],h=document.getElementById('history');h.innerHTML=list.length?list.map(x=>`<div class="signalrow"><b class="${x.side==='UP'?'up':'down'}">${x.side}</b> · score ${Math.round(x.score*100)}% · lead ${fmt(x.lead_bps)} bps · ${new Date(x.timestamp).toLocaleTimeString()}</div>`).join(''):'No signals yet.';}async function poll(){try{let r=await fetch('/api/state',{cache:'no-store'});render(await r.json())}catch(e){document.getElementById('status').textContent='Connection lost'} }poll();setInterval(poll,3000);</script></body></html>'''

_feed_thread = threading.Thread(target=feed_loop, name='btc-price-feed', daemon=True)
_feed_thread.start()

if __name__ == '__main__':
    port = int(__import__('os').environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
