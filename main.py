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
                logging.debug("No create/update operations found in commit")
                return {}

            # Let's try a different approach with direct indexing into the message
            # Save original message for debugging
            original_message = message

            # IMPORTANT: The CAR data is directly appended after the CBOR encoded commit
            # So we need to find where the commit CBOR ends and the CAR begins
            try:
                # First CBOR message is the header
                header = cbor2.loads(message)
                header_bytes = cbor2.dumps(header)

                # Skip past the header
                message = message[len(header_bytes) :]

                # Second CBOR message is the commit
                commit = cbor2.loads(message)
                commit_bytes = cbor2.dumps(commit)

                # The remaining data should be the CAR
                car_data = message[len(commit_bytes) :]

                logging.info(f"Found CAR data of size: {len(car_data)} bytes")

                # Write out CAR data to a file for inspection (temporary debugging)
                with open("debug_car_data.bin", "wb") as f:
                    f.write(car_data)
                    logging.info("Wrote CAR data to debug_car_data.bin")

            except Exception as e:
                logging.error(f"Error separating CAR data: {e}")
                return {}

            # Now let's try a simpler approach to CAR parsing
            records = {}
            try:
                # The CAR format starts with a specific format
                # Try to directly find the blocks

                # Skip the CAR header (search for the first CID directly)
                # Most implementations expect a varint-encoded size followed by data
                # Since we're having trouble with the standard parsing,
                # let's try a brute force approach

                # Look for blocks using known IPLD CID prefixes
                for op in ops_with_records:
                    path = op.get("path")
                    cid = op.get("cid")

                    if not cid or not isinstance(cid, cbor2.CBORTag):
                        continue

                    # Convert CBOR tag to bytes for search
                    cid_bytes = None
                    if hasattr(cid.value, "hex"):
                        cid_hex = cid.value.hex()
                        logging.info(f"Looking for CID hex: {cid_hex} for path: {path}")

                        # Try to find this CID in the CAR data - it should be there
                        # with some prefix bytes

                        # For debugging, let's try a manual approach
                        # Output all the path and CID information
                        pretty_op = json.dumps(op, cls=CBORTagEncoder)
                        logging.info(f"Operation for path {path}: {pretty_op}")

                        # Try directly decoding the car_data as CBOR as well, in case
                        # it's not in CAR format but just another CBOR message
                        try:
                            direct_record = cbor2.loads(car_data)
                            logging.info(
                                f"Direct CBOR decode of CAR data worked: {direct_record}"
                            )
                            # If this worked, use it
                            if isinstance(direct_record, dict):
                                records[path] = direct_record
                                logging.info(
                                    f"Added record for {path} from direct CBOR decode"
                                )
                        except Exception as e:
                            logging.debug(f"Direct CBOR decode failed: {e}")

                return records

            except Exception as e:
                logging.error(f"Failed to extract blocks from CAR: {e}")
                return {}

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

    def pretty_print_record(self, record_type, record):
        """Format and print record contents based on its type"""
        try:
            if record_type == "app.bsky.feed.post":
                creator_id = record.get("repo", "Unknown")

                # Extract post text safely
                text = "No text"
                if "text" in record:
                    text = record["text"]
                elif (
                    isinstance(record, dict)
                    and "record" in record
                    and isinstance(record["record"], dict)
                ):
                    text = record["record"].get("text", "No text")

                # Format and print the post
                logging.info(f"📝 POST from {creator_id}: {text}")

                # Print additional details if present
                if (
                    isinstance(record, dict)
                    and "record" in record
                    and isinstance(record["record"], dict)
                ):
                    if "langs" in record["record"]:
                        logging.info(f"  Language: {record['record']['langs']}")
                    if "embed" in record["record"]:
                        embed_type = record["record"]["embed"].get(
                            "$type", "unknown embed"
                        )
                        logging.info(f"  Has embed: {embed_type}")

            elif record_type == "app.bsky.feed.like":
                creator_id = record.get("repo", "Unknown")
                subject = None

                if (
                    isinstance(record, dict)
                    and "record" in record
                    and isinstance(record["record"], dict)
                ):
                    if "subject" in record["record"]:
                        subject = record["record"]["subject"]

                if subject:
                    target_uri = subject.get("uri", "unknown")
                    logging.info(f"👍 LIKE from {creator_id} for {target_uri}")
                else:
                    logging.info(f"👍 LIKE from {creator_id} for unknown content")

            elif record_type == "app.bsky.feed.repost":
                creator_id = record.get("repo", "Unknown")
                subject = None

                if (
                    isinstance(record, dict)
                    and "record" in record
                    and isinstance(record["record"], dict)
                ):
                    if "subject" in record["record"]:
                        subject = record["record"]["subject"]

                if subject:
                    target_uri = subject.get("uri", "unknown")
                    logging.info(f"🔄 REPOST from {creator_id} of {target_uri}")
                else:
                    logging.info(f"🔄 REPOST from {creator_id} of unknown content")

            elif record_type == "app.bsky.graph.follow":
                creator_id = record.get("repo", "Unknown")
                subject = None

                if (
                    isinstance(record, dict)
                    and "record" in record
                    and isinstance(record["record"], dict)
                ):
                    if "subject" in record["record"]:
                        subject = record["record"]["subject"]

                if subject:
                    target_did = subject.get("did", "unknown")
                    logging.info(f"👤 FOLLOW from {creator_id} to {target_did}")
                else:
                    logging.info(f"👤 FOLLOW from {creator_id} to unknown user")

            else:
                # For other types, print a summary
                logging.info(
                    f"📄 {record_type.upper()} record from {record.get('repo', 'Unknown')}"
                )

        except Exception as e:
            logging.error(f"Error pretty printing record: {e}")
            # Fall back to basic JSON
            pretty_record = json.dumps(record, indent=2, cls=CBORTagEncoder)
            logging.info(f"Record content: {pretty_record[:200]}...")

    async def _handle_messages(self):
        try:
            async for message in self.websocket:
                try:
                    # Log the raw message size for debugging
                    logging.info(f"Received message of size: {len(message)} bytes")

                    # Let's log the first few bytes to understand the structure
                    logging.info(f"Message starts with: {message[:50].hex()}")

                    # Try to decode the first CBOR message
                    try:
                        first_cbor = cbor2.loads(message)
                        logging.info(f"First CBOR object: {first_cbor}")

                        # Move past the first CBOR object
                        first_bytes = cbor2.dumps(first_cbor)
                        remaining = message[len(first_bytes) :]

                        # Try to decode the second CBOR message
                        try:
                            second_cbor = cbor2.loads(remaining)
                            logging.info(f"Second CBOR object: {second_cbor}")

                            # Look specifically for post content in this message
                            if isinstance(second_cbor, dict) and "ops" in second_cbor:
                                for op in second_cbor["ops"]:
                                    if op.get(
                                        "action"
                                    ) == "create" and "app.bsky.feed.post" in op.get(
                                        "path", ""
                                    ):
                                        logging.info(f"Found a post creation op: {op}")

                                        # Extract the CID
                                        cid = op.get("cid")
                                        if cid:
                                            logging.info(f"Post CID: {cid}")

                                            # Skip past the second CBOR object
                                            second_bytes = cbor2.dumps(second_cbor)
                                            remaining2 = remaining[len(second_bytes) :]

                                            # The rest should be the CAR data
                                            logging.info(
                                                f"CAR data size: {len(remaining2)} bytes"
                                            )

                                            # Try direct approach - dump the data to a file for inspection
                                            with open("post_car_data.bin", "wb") as f:
                                                f.write(remaining2)
                                                logging.info(
                                                    f"Wrote CAR data to post_car_data.bin"
                                                )

                                            # Try if it's actually a CBOR object directly
                                            try:
                                                third_cbor = cbor2.loads(remaining2)
                                                logging.info(
                                                    f"Direct decode of 'CAR' worked: {third_cbor}"
                                                )

                                                # Check if this has the post text
                                                if (
                                                    isinstance(third_cbor, dict)
                                                    and "text" in third_cbor
                                                ):
                                                    logging.info(
                                                        f"POST TEXT FOUND: {third_cbor['text']}"
                                                    )
                                            except Exception as e:
                                                logging.debug(
                                                    f"Direct CBOR decode of CAR failed: {e}"
                                                )

                        except Exception as e:
                            logging.debug(f"Second CBOR decode failed: {e}")

                    except Exception as e:
                        logging.debug(f"First CBOR decode failed: {e}")

                    # Regular processing
                    decoded_value = self.decode_message(message)

                    if decoded_value:
                        repo = decoded_value["commit"].get("repo", "")
                        seq = decoded_value["commit"].get("seq", 0)

                        logging.info(f"Processing commit from repo {repo} seq {seq}")

                        # Process operations
                        for op in decoded_value["commit"].get("ops", []):
                            action = op["action"]
                            path = op["path"]
                            logging.info(f" - {action} {path}")

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
