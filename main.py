import asyncio
import websockets
import json
import logging
from quixstreams import Application
from pprint import pformat
import cbor2
from datetime import datetime
import io
import base64
import struct


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


class SimpleCARParser:
    """Simplified parser for Content Addressable aRchive (CAR) format"""

    def __init__(self):
        self.blocks = {}

    def parse(self, data):
        """Parse a CAR file from bytes"""
        stream = io.BytesIO(data)

        # Skip header - read the header length first (8 bytes)
        header_len_bytes = stream.read(8)
        if not header_len_bytes or len(header_len_bytes) < 8:
            return self.blocks

        header_len = int.from_bytes(header_len_bytes, byteorder="big")
        # Skip the header content
        stream.read(header_len)

        # Parse blocks
        while True:
            try:
                # Read block length
                block_length_data = stream.read(8)
                if not block_length_data or len(block_length_data) < 8:
                    break

                block_length = int.from_bytes(block_length_data, byteorder="big")

                # Read CID length and CID
                # First read one byte for CID length
                cid_length_data = stream.read(1)
                if not cid_length_data:
                    break

                cid_length = cid_length_data[0]
                cid_data = stream.read(cid_length)
                if len(cid_data) < cid_length:
                    break

                # Read block data
                block_data = stream.read(block_length - cid_length - 1)
                if len(block_data) < (block_length - cid_length - 1):
                    break

                # Store block with CID as key
                # Using hex representation of CID as key
                cid_hex = cid_data.hex()
                self.blocks[cid_hex] = block_data

                logging.debug(f"Parsed block with CID: {cid_hex[:10]}...")

            except Exception as e:
                logging.error(f"Error parsing CAR block: {e}")
                break

        return self.blocks


class WebSocketClient:
    def __init__(self, uri: str, producer, topic: str):
        self.uri = uri
        self.producer = producer
        self.topic = topic
        self.websocket = None
        self.running = False
        self.car_parser = SimpleCARParser()

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

    def parse_car_blocks(self, data):
        """Parse CAR blocks from binary data"""
        try:
            blocks = self.car_parser.parse(data)
            return blocks
        except Exception as e:
            logging.error(f"Failed to parse CAR blocks: {e}")
            return {}

    def decode_record_cbor(self, data):
        """Decode a record from CBOR data"""
        try:
            record = cbor2.loads(data)
            return record
        except Exception as e:
            logging.error(f"Failed to decode record CBOR: {e}")
            return None

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
                "cid": op.get("cid"),
            }
            cleaned["ops"].append(cleaned_op)

        return cleaned

    def extract_record_data(self, message, commit_data):
        """Extract and parse the actual record data from the message"""
        try:
            # Get ops with create or update actions
            ops_with_records = [
                op
                for op in commit_data.get("ops", [])
                if op.get("action") in ["create", "update"]
            ]

            if not ops_with_records:
                return {}

            # Get the car blocks from the message
            # The CAR data is expected to be after the two CBOR messages (header and commit)
            header_len = len(cbor2.dumps(cbor2.loads(message)))
            remaining = self.find_second_cbor(message)
            if not remaining:
                return {}

            commit_len = len(cbor2.dumps(cbor2.loads(remaining)))
            car_data_start = header_len + commit_len
            car_data = message[car_data_start:]

            if not car_data:
                return {}

            # Parse the CAR blocks
            blocks = self.parse_car_blocks(car_data)

            records = {}
            for op in ops_with_records:
                path = op.get("path")
                cid = op.get("cid")

                if not cid:
                    continue

                # Convert CBOR tag to hex for lookup
                if isinstance(cid, cbor2.CBORTag) and cid.tag == 42:
                    cid_hex = cid.value.hex()

                    if cid_hex in blocks:
                        block_data = blocks[cid_hex]
                        try:
                            # Decode the block data as CBOR
                            record = self.decode_record_cbor(block_data)
                            if record:
                                records[path] = record
                                logging.info(f"Extracted record for {path}: {record}")
                        except Exception as e:
                            logging.error(f"Failed to decode record for {path}: {e}")

            return records

        except Exception as e:
            logging.error(f"Failed to extract record data: {e}")
            logging.exception("Exception details:")
            return {}

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

                    # Extract record data
                    record_data = self.extract_record_data(message, commit_data)

                    if cleaned_commit:
                        result = {
                            "header": {"type": header.get("t"), "op": header.get("op")},
                            "commit": cleaned_commit,
                        }

                        # Add record data if available
                        if record_data:
                            result["records"] = record_data

                        return result
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
                    # Log the raw message size for debugging
                    logging.debug(f"Received message of size: {len(message)} bytes")

                    decoded_value = self.decode_message(message)

                    if decoded_value:
                        repo = decoded_value["commit"].get("repo", "")
                        seq = decoded_value["commit"].get("seq", 0)

                        logging.info(f"Processing commit from repo {repo} seq {seq}")

                        # Log operations
                        for op in decoded_value["commit"].get("ops", []):
                            action = op["action"]
                            path = op["path"]
                            logging.info(f" - {action} {path}")

                            # If we have record data for this path, log it
                            if (
                                "records" in decoded_value
                                and path in decoded_value["records"]
                            ):
                                record = decoded_value["records"][path]
                                # Pretty print record data
                                pretty_record = json.dumps(
                                    record, indent=2, cls=CBORTagEncoder
                                )
                                logging.info(f"   Record content: {pretty_record}")

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
                    logging.exception("Exception details:")
                    # Continue processing other messages
                    continue

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
    # Set up more detailed logging
    logging.basicConfig(
        level="INFO", format="%(asctime)s - %(levelname)s - %(message)s"
    )
    # Set the logger for the websockets library to WARNING to reduce noise
    logging.getLogger("websockets").setLevel(logging.WARNING)
    asyncio.run(main())

