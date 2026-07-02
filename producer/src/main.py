import asyncio
import json
import websockets

SYMBOL = "btcusdt"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth"

async def main():
    print(f"[*] Connecting to Binance WebSocket: {SYMBOL.upper()} orderbook")
    async for websocket in websockets.connect(WS_URL):
        try:
            async for message in websocket:
                data = json.loads(message)
                print(f"[{data['E']}] bids={len(data['b'])} asks={len(data['a'])} | first bid: {data['b'][0]}")
        except websockets.ConnectionClosed:
            print("[!] Connection closed, reconnecting...")
            continue

if __name__ == "__main__":
    asyncio.run(main())