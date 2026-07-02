import asyncio
import json
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

app = FastAPI()

REDIS_HOST = "redis-master.data-pipeline.svc.cluster.local"
REDIS_PORT = 6379

async def get_redis():
    return aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ── REST 엔드포인트 ──────────────────────────────────────────────

@app.get("/imbalance/{symbol}")
async def get_imbalance(symbol: str):
    r = await get_redis()
    detail = await r.hgetall(f"imbalance:detail:{symbol.upper()}")
    await r.aclose()
    if not detail:
        return {"error": f"No data for {symbol}"}
    return {
        "symbol": symbol.upper(),
        "imbalance": float(detail.get("imbalance", 0)),
        "weighted_bids": float(detail.get("weighted_bids", 0)),
        "weighted_asks": float(detail.get("weighted_asks", 0)),
        "window_start": int(detail.get("window_start", 0)),
        "window_end": int(detail.get("window_end", 0)),
    }

# ── WebSocket 엔드포인트 ─────────────────────────────────────────

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await websocket.accept()
    r = await get_redis()
    try:
        while True:
            detail = await r.hgetall(f"imbalance:detail:{symbol.upper()}")
            if detail:
                await websocket.send_json({
                    "symbol": symbol.upper(),
                    "imbalance": float(detail.get("imbalance", 0)),
                    "weighted_bids": float(detail.get("weighted_bids", 0)),
                    "weighted_asks": float(detail.get("weighted_asks", 0)),
                    "window_end": int(detail.get("window_end", 0)),
                })
            await asyncio.sleep(1)
    except Exception:
        pass
    finally:
        await r.aclose()

# ── 대시보드 HTML ────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>OrderFlow Dashboard</title>
    <style>
        body { font-family: monospace; background: #0f0f0f; color: #e0e0e0; padding: 40px; }
        h1 { color: #00ff88; }
        .card { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 24px; max-width: 400px; }
        .label { color: #888; font-size: 12px; margin-top: 12px; }
        .value { font-size: 28px; font-weight: bold; margin-top: 4px; }
        .bar-container { background: #333; border-radius: 4px; height: 12px; margin-top: 8px; overflow: hidden; }
        .bar { height: 100%; border-radius: 4px; transition: width 0.3s; }
        .status { font-size: 12px; color: #555; margin-top: 16px; }
    </style>
</head>
<body>
    <h1>OrderFlow — Real-time Orderbook Imbalance</h1>
    <div class="card">
        <div class="label">SYMBOL</div>
        <div class="value" id="symbol">BTCUSDT</div>

        <div class="label">IMBALANCE (-1 ~ +1)</div>
        <div class="value" id="imbalance">-</div>
        <div class="bar-container">
            <div class="bar" id="bar" style="width:50%; background:#888;"></div>
        </div>

        <div class="label">WEIGHTED BIDS</div>
        <div class="value" id="bids" style="color:#00ff88;">-</div>

        <div class="label">WEIGHTED ASKS</div>
        <div class="value" id="asks" style="color:#ff4444;">-</div>

        <div class="status" id="status">Connecting...</div>
    </div>

    <script>
        const ws = new WebSocket(`ws://${location.host}/ws/BTCUSDT`);

        ws.onmessage = (e) => {
            const d = JSON.parse(e.data);
            const imb = d.imbalance;

            document.getElementById('imbalance').textContent = imb.toFixed(4);
            document.getElementById('bids').textContent = d.weighted_bids.toFixed(4);
            document.getElementById('asks').textContent = d.weighted_asks.toFixed(4);

            // 게이지 바: -1~+1을 0~100%로 변환
            const pct = ((imb + 1) / 2 * 100).toFixed(1);
            const bar = document.getElementById('bar');
            bar.style.width = pct + '%';
            bar.style.background = imb > 0 ? '#00ff88' : '#ff4444';

            const t = new Date(d.window_end).toLocaleTimeString();
            document.getElementById('status').textContent = `Last update: ${t}`;
        };

        ws.onopen = () => document.getElementById('status').textContent = 'Connected';
        ws.onclose = () => document.getElementById('status').textContent = 'Disconnected';
    </script>
</body>
</html>
"""