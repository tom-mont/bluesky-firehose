import asyncio
import websockets
import json
import logging
from quixstreams import Application
from pprint import pformat
from typing import Optional
import cbor2  # Make sure to pip install cbor2


class WebSocketClient:
    def __init__(self, uri: str, producer, topic: str):
        self.uri = uri
        self.producer = producer
        self.topic = topic
        self.websocket = None
        self.running = False

    async def connect(self):
        """Establish WebSocket connection with retry logic"""
        while self.running:
            try:
                async with websockets.connect(self.uri) as websocket:
                    self.websocket = websocket
                    logging.info(f"Connected to WebSocket at {self.uri}")
                    await self._handle_messages()
            except Exception as e:
                logging.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)  # Wait 5 seconds before reconnecting

    def decode_repo_ops(self, message):
        """Decode CBOR message and extract repo operations"""
        try:
            # Decode the binary message using CBOR
            decoded = cbor2.loads(message)

            # The Bluesky firehose format typically includes:
            # - seq: sequence number
            # - repo: the repo (user) that was modified
            # - ops: array of operations that occurred
            # - time: timestamp

            return {
                "seq": decoded.get("seq"),
                "repo": decoded.get("repo"),
                "ops": decoded.get("ops", []),
                "time": decoded.get("time"),
            }
        except Exception as e:
            logging.error(f"Failed to decode CBOR message: {e}")
            return None

    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                try:
                    # Message comes as binary data, need to decode with CBOR
                    decoded_value = self.decode_repo_ops(message)

                    if decoded_value:
                        key = str(decoded_value.get("seq", ""))

                        logging.debug("Got: %s", pformat(decoded_value))

                        # Send to kafka via producer
                        self.producer.produce(
                            topic=self.topic,
                            key=key,
                            value=json.dumps(decoded_value),
                        )
                except Exception as e:
                    logging.error(f"Failed to process message: {e}")
        except websockets.ConnectionClosed:
            logging.info("WebSocket connection closed")

    async def start(self):
        """Start the WebSocket client"""
        self.running = True
        await self.connect()

    async def stop(self):
        """Stop the WebSocket client"""
        self.running = False
        if self.websocket:
            await self.websocket.close()


def handle_stats(stats_msg):
    stats = json.loads(stats_msg)
    logging.info("STATS: %s", pformat(stats))


async def main():
    logging.info("START")
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
        producer_extra_config={
            "statistics.interval.ms": 3 * 1000,
            "stats_cb": handle_stats,
            "debug": "msg",
            "linger.ms": 200,
            "batch.size": 1024 * 1024,
            "compression.type": "gzip",
        },
    )

    with app.get_producer() as producer:
        client = WebSocketClient(
            uri="wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
            producer=producer,
            topic="bsky_events",  # Adjust topic name as needed
        )

        await client.start()


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    asyncio.run(main())
