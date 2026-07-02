import asyncio
import aiohttp
import websockets
import json

SYMBOL = "btcusdt"
SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth"

class OrderBook:
    def __init__(self):
        self.bids = {}  # 매수: {price: quantity}
        self.asks = {}  # 매도: {price: quantity}
        self.last_update_id = 0
        self.ready = False

    def apply_snapshot(self, snapshot: dict):
        """REST 스냅샷으로 오더북 초기화"""
        self.bids = {p: q for p, q in snapshot["bids"]}
        self.asks = {p: q for p, q in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]
        self.ready = True
        print(f"[snapshot] lastUpdateId={self.last_update_id} | bids={len(self.bids)} asks={len(self.asks)}")

    def apply_diff(self, event: dict) -> bool:
        """
        diff 이벤트를 오더북에 적용.
        시퀀스 갭 감지 시 False 반환 → 재동기화 트리거.
        """
        first_update_id = event["U"]  # 이 이벤트의 첫 번째 updateId
        final_update_id = event["u"]  # 이 이벤트의 마지막 updateId

        # 스냅샷보다 오래된 이벤트는 무시
        if final_update_id <= self.last_update_id:
            return True

        # 시퀀스 연속성 검증 — 갭이 있으면 재동기화 필요
        if first_update_id > self.last_update_id + 1:
            print(f"[!] GAP detected: expected={self.last_update_id + 1}, got={first_update_id} → resync")
            self.ready = False
            return False

        # 매수/매도 호가 업데이트
        for price, qty in event["b"]:
            if qty == "0.00000000":
                self.bids.pop(price, None)  # 수량 0 = 해당 호가 삭제
            else:
                self.bids[price] = qty

        for price, qty in event["a"]:
            if qty == "0.00000000":
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = final_update_id
        return True

    def top(self, n=5):
        """상위 n개 매수/매도 호가 반환"""
        top_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:n]
        top_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:n]
        return top_bids, top_asks


async def fetch_snapshot() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(SNAPSHOT_URL) as resp:
            return await resp.json()


async def main():
    ob = OrderBook()
    buffer = []  # 스냅샷 받기 전 diff를 여기 쌓아둠

    print(f"[*] Connecting to Binance WebSocket: {SYMBOL.upper()}")

    async for websocket in websockets.connect(WS_URL):
        try:
            # 1단계: diff 스트림 구독 시작, 버퍼링
            print("[*] Buffering diffs before snapshot...")
            ob.ready = False
            buffer.clear()

            # diff 몇 개 버퍼에 쌓는 동안 스냅샷 요청
            async def buffer_and_snapshot():
                # diff 몇 개 먼저 버퍼에 넣기
                for _ in range(10):
                    msg = await websocket.recv()
                    buffer.append(json.loads(msg))

                # 스냅샷 받기
                print("[*] Fetching snapshot...")
                snapshot = await fetch_snapshot()
                ob.apply_snapshot(snapshot)

                # 스냅샷보다 오래된 버퍼 이벤트 걸러내고 적용
                for event in buffer:
                    ob.apply_diff(event)
                buffer.clear()
                print("[*] Orderbook ready!")

            await buffer_and_snapshot()

            # 2단계: 이후 diff 실시간 적용
            async for message in websocket:
                event = json.loads(message)

                if not ob.ready:
                    # 재동기화 필요 — 연결 끊고 다시
                    print("[!] Resyncing...")
                    break

                ok = ob.apply_diff(event)
                if not ok:
                    break  # 갭 감지 → 외부 루프에서 재연결

                # 상위 3개 호가 출력
                top_bids, top_asks = ob.top(3)
                print(f"\n[{event['E']}] lastUpdateId={ob.last_update_id}")
                print(f"  BIDS: {top_bids}")
                print(f"  ASKS: {top_asks}")

        except websockets.ConnectionClosed:
            print("[!] Connection closed, reconnecting...")
            continue

if __name__ == "__main__":
    asyncio.run(main())