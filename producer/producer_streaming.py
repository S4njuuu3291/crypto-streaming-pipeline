import asyncio
import json
from logging import exception
from multiprocessing.pool import AsyncResult
from select import poll

import websockets
import yaml
from confluent_kafka import Producer
from models import PriceEvent


async def consume_binance_ws(symbol: str):
    producer = Producer(producer_confs)
    ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    while True:
        try:
            print(f"Connecting to Binance WebSocket for {symbol}...")
            async with websockets.connect(
                ws_url, open_timeout=20, ping_interval=20
            ) as ws:
                print(f"✅ Connected to Binance WebSocket for {symbol}")

                while True:
                    msg = await ws.recv()
                    raw_event = json.loads(msg)
                    try:
                        price = PriceEvent.from_binance(raw_event)
                    except Exception as e:
                        print(f"⚠️ Invalid event for {symbol} skipped: {e}")
                        continue

                    producer.produce(
                        KAFKA_TOPIC,
                        key=price.symbol,
                        value=price.model_dump_json(),
                    )
                    producer.poll(0)
                    print(f"Produced event to Kafka: {price.symbol} at {price.price}")
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
            print(f"⚠️ Connection error for {symbol}: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(
                f"❌ Unexpected error for {symbol}: {e}. Reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)


if __name__ == "__main__":
    config = yaml.safe_load(open(".//config/settings.yaml"))

    symbols = config.get("symbols")
    bootstrap_servers = config.get("kafka").get("bootstrap_servers")

    KAFKA_TOPIC = config.get("kafka").get("topic")
    BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/ethusdt@trade"

    producer_confs = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "retries": 5,
        "linger.ms": 100,
        "batch.size": 16384,
        "enable.idempotence": True,
    }

    try:
        loop = asyncio.get_event_loop()
        tasks = [consume_binance_ws(symbol) for symbol in symbols]
        loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        print("Shutting down...")
