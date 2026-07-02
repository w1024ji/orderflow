import asyncio
import json
import io
import requests
import websockets
import aiohttp
import fastavro
from fastavro.schema import load_schema
from confluent_kafka import Producer
import os

SYMBOL = "btcusdt"
SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
WS_URL = f"wss://stream.binance.com:9443/ws/{SYMBOL}@depth"
# SCHEMA_REGISTRY_URL = "http://localhost:8081"
# KAFKA_BROKER = "kafka.data-pipeline.svc.cluster.local:9092"
# KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry.data-pipeline.svc.cluster.local:8081"
KAFKA_BROKER = "kafka.data-pipeline.svc.cluster.local:9092"
TOPIC = "orderbook.raw"

# BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "orderbook.avsc")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "orderbook.avsc")

# ── Schema Registry ──────────────────────────────────────────────
def register_schema(schema_str: str) -> int:
    subject = f"{TOPIC}-value"
    resp = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": schema_str}
    )
    resp.raise_for_status()
    schema_id = resp.json()["id"]
    print(f"[schema] registered id={schema_id}")
    return schema_id


def avro_serialize(schema, schema_id: int, record: dict) -> bytes:
    """Confluent wire format: magic byte(0) + schema_id(4 bytes) + avro payload"""
    buf = io.BytesIO()
    buf.write(b'\x00')
    buf.write(schema_id.to_bytes(4, 'big'))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


# ── OrderBook ────────────────────────────────────────────────────
class OrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.last_update_id = 0
        self.ready = False

    def apply_snapshot(self, snapshot: dict):
        self.bids = {p: q for p, q in snapshot["bids"]}
        self.asks = {p: q for p, q in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]
        self.ready = True
        print(f"[snapshot] lastUpdateId={self.last_update_id} | bids={len(self.bids)} asks={len(self.asks)}")

    def apply_diff(self, event: dict) -> bool:
        first_update_id = event["U"]
        final_update_id = event["u"]

        if final_update_id <= self.last_update_id:
            return True

        if first_update_id > self.last_update_id + 1:
            print(f"[!] GAP detected: expected={self.last_update_id + 1}, got={first_update_id} → resync")
            self.ready = False
            return False

        for price, qty in event["b"]:
            if qty == "0.00000000":
                self.bids.pop(price, None)
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
        top_bids = sorted(self.bids.items(), key=lambda x: float(x[0]), reverse=True)[:n]
        top_asks = sorted(self.asks.items(), key=lambda x: float(x[0]))[:n]
        return top_bids, top_asks

    def to_record(self, event_time: int) -> dict:
        top_bids, top_asks = self.top(20)
        return {
            "symbol": SYMBOL.upper(),
            "event_time": event_time,
            "last_update_id": self.last_update_id,
            "bids": [{"price": p, "quantity": q} for p, q in top_bids],
            "asks": [{"price": p, "quantity": q} for p, q in top_asks],
        }


# ── Kafka Producer ───────────────────────────────────────────────
def make_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "enable.idempotence": True,   # idempotent producer
        "acks": "all",
    })


def delivery_report(err, msg):
    if err:
        print(f"[kafka] delivery failed: {err}")
    else:
        print(f"[kafka] delivered → partition={msg.partition()} offset={msg.offset()}")


# ── Main ─────────────────────────────────────────────────────────
async def fetch_snapshot() -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(SNAPSHOT_URL) as resp:
            return await resp.json()


async def main():
    # 스키마 로드 & 등록
    schema = load_schema(SCHEMA_PATH)
    with open(SCHEMA_PATH) as f:
        schema_id = register_schema(f.read())

    # Kafka producer 초기화
    producer = make_kafka_producer()

    ob = OrderBook()
    buffer = []

    print(f"[*] Connecting to Binance WebSocket: {SYMBOL.upper()}")

    async for websocket in websockets.connect(WS_URL):
        try:
            ob.ready = False
            buffer.clear()

            # 버퍼링 + 스냅샷
            print("[*] Buffering diffs...")
            for _ in range(10):
                msg = await websocket.recv()
                buffer.append(json.loads(msg))

            print("[*] Fetching snapshot...")
            snapshot = await fetch_snapshot()
            ob.apply_snapshot(snapshot)

            for event in buffer:
                ob.apply_diff(event)
            buffer.clear()
            print("[*] Orderbook ready! Publishing to Kafka...")

            # 실시간 diff 적용 + Kafka 발행
            async for message in websocket:
                event = json.loads(message)

                if not ob.ready:
                    print("[!] Resyncing...")
                    break

                ok = ob.apply_diff(event)
                if not ok:
                    break

                # Avro 직렬화
                record = ob.to_record(event["E"])
                payload = avro_serialize(schema, schema_id, record)

                # Kafka 발행 (symbol을 key로 → 같은 파티션 보장)
                producer.produce(
                    topic=TOPIC,
                    key=SYMBOL.upper(),
                    value=payload,
                    callback=delivery_report
                )
                producer.poll(0)  # 비동기 콜백 처리

                # 상위 3개 호가 출력
                top_bids, top_asks = ob.top(3)
                print(f"[{event['E']}] id={ob.last_update_id} | bid={top_bids[0][0]} ask={top_asks[0][0]}")

        except websockets.ConnectionClosed:
            print("[!] Connection closed, reconnecting...")
            producer.flush()
            continue


if __name__ == "__main__":
    asyncio.run(main())