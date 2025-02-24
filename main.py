import asyncio
import websockets
import json
import logging
from quixstreams import Application
from pprint import pformat
from typing import Optional
import cbor2
from datetime import datetime


class CBORTagEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, cbor2.CBORTag):
            # For CIDs and other CBOR tags, convert to hex string
            return f"tag:{obj.tag}:{obj.value.hex()}"
        if isinstance(obj, bytes):
            return self.bytes_to_hex(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

    @staticmethod
    def bytes_to_hex(b):
        return b.hex()


class WebSocketClient:
    def __init__(self, uri: str, producer, topic: str):
        self.uri = uri
        self.producer = producer
        self.topic = topic
        self.websocket = None
        self.running = False

    async def connect(self):
        while self.running:
            try:
                async with websockets.connect(self.uri) as websocket:
                    self.websocket = websocket
                    logging.info(f"Connected to WebSocket at {self.uri}")
                    await self._handle_messages()
            except Exception as e:
                logging.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)

    def find_second_cbor(self, data):
        """Find the start of the second CBOR message in the binary data"""
        try:
            first_msg = cbor2.loads(data)
            first_len = len(cbor2.dumps(first_msg))
            return data[first_len:]
        except Exception as e:
            logging.error(f"Error finding second CBOR message: {e}")
            return None

    def clean_commit_data(self, commit_data):
        """Clean and structure the commit data for better usability"""
        if not commit_data:
            return None

        # Extract the relevant fields and clean the data
        cleaned = {
            "repo": commit_data.get("repo"),
            "seq": commit_data.get("seq"),
            "rev": commit_data.get("rev"),
            "since": commit_data.get("since"),
            "time": commit_data.get("time"),
            "ops": [],
        }

        # Clean up the operations
        for op in commit_data.get("ops", []):
            cleaned_op = {
                "action": op.get("action"),
                "path": op.get("path"),
                "cid": op.get(
                    "cid"
                ),  # Will be encoded as tag:42:hexstring by CBORTagEncoder
            }
            cleaned["ops"].append(cleaned_op)

        return cleaned

    def decode_message(self, message):
        """Decode both parts of the message"""
        try:
            # Decode the header
            header = cbor2.loads(message)

            if header.get("t") != "#commit":
                return None

            # Find and decode the commit data
            remaining = self.find_second_cbor(message)
            if remaining:
                try:
                    commit_data = cbor2.loads(remaining)
                    cleaned_commit = self.clean_commit_data(commit_data)

                    if cleaned_commit:
                        return {
                            "header": {"type": header.get("t"), "op": header.get("op")},
                            "commit": cleaned_commit,
                        }
                except Exception as e:
                    logging.error(f"Failed to decode commit data: {e}")

            return None

        except Exception as e:
            logging.error(f"Failed to decode message: {e}")
            return None

    async def _handle_messages(self):
        try:
            async for message in self.websocket:
                try:
                    decoded_value = self.decode_message(message)

                    if decoded_value:
                        repo = decoded_value["commit"].get("repo", "")
                        seq = decoded_value["commit"].get("seq", 0)

                        logging.info(f"Processing commit from repo {repo} seq {seq}")

                        for op in decoded_value["commit"].get("ops", []):
                            logging.info(f" - {op['action']} {op['path']}")

                        # Serialize with custom encoder
                        json_value = json.dumps(decoded_value, cls=CBORTagEncoder)

                        # Use repo as key for better Kafka partitioning
                        self.producer.produce(
                            topic=self.topic,
                            key=repo,
                            value=json_value,
                        )

                except Exception as e:
                    logging.error(f"Failed to process message: {e}")
                    raise

        except websockets.ConnectionClosed:
            logging.info("WebSocket connection closed")

    async def start(self):
        self.running = True
        await self.connect()

    async def stop(self):
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
            topic="bsky_events",
        )

        await client.start()


if __name__ == "__main__":
    logging.basicConfig(
        level="DEBUG", format="%(asctime)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())

