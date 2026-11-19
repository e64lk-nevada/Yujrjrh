EdgeMarket v2 — Render / iPhone

FILES (archive root, no folders):
- app.py: Flask dashboard + signal engine
- requirements.txt: dependencies
- render.yaml: Render service configuration
- README_UA.txt: this guide

Deploy/update on Render: upload these files to your GitHub repository root, commit, and let Render deploy.
Build command: pip install -r requirements.txt
Start command: gunicorn app:app --workers 1 --threads 4 --timeout 120

DATA SOURCES:
- Binance BTC/USDT is the live 2-second signal feed and defines Price to Beat/window values.
- Chainlink standard BTC/USD Ethereum Mainnet feed is shown as a reference cross-check, polled every 60 seconds.
- IMPORTANT: this standard Chainlink reference feed is NOT Polymarket's Chainlink BTC/USD TWAP60 stream. Therefore this build does not guarantee exact Polymarket settlement-price matching. The TWAP60 stream is not integrated in this free build.

Signals: begins at T+180s, uses lead, 5s/12s momentum and volatility filter. Score is a heuristic, not calibrated win probability. Signals are experimental; use paper trading only.

Render free services may sleep.
