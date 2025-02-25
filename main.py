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
    """Enhanced parser for Content Addressable aRchive (CAR) format used by AT Protocol"""

    def __init__(self):
        self.blocks = {}

    def parse(self, data):
        """Parse a CAR file from bytes"""
        stream = io.BytesIO(data)

        try:
            # Try to read the CAR header
            # First 8 bytes are the header length
            header_len_bytes = stream.read(8)
            if not header_len_bytes or len(header_len_bytes) < 8:
                logging.warning("CAR data too short to read header length")
                return self.blocks

            header_len = int.from_bytes(header_len_bytes, byteorder="big")
            logging.debug(f"CAR header length: {header_len}")

            # Read the header content
            header_content = stream.read(header_len)
            if len(header_content) < header_len:
                logging.warning("Incomplete CAR header")
                return self.blocks

            # Try to parse the header as CBOR (AT Protocol uses CBOR for CAR headers)
            try:
                header = cbor2.loads(header_content)
                logging.debug(f"CAR header: {header}")
            except Exception as e:
                logging.warning(f"Failed to parse CAR header as CBOR: {e}")
                # Continue anyway as we might still be able to parse blocks

            # Now parse the blocks
            while True:
                # Read block length (8 bytes)
                block_length_data = stream.read(8)
                if not block_length_data or len(block_length_data) < 8:
                    break

                block_length = int.from_bytes(block_length_data, byteorder="big")
                if block_length <= 0:
                    logging.warning(f"Invalid block length: {block_length}")
                    break

                # Read the block data
                block_data = stream.read(block_length)
                if len(block_data) < block_length:
                    logging.warning(
                        f"Incomplete block data: got {len(block_data)}, expected {block_length}"
                    )
                    break

                # AT Protocol block structure is:
                # - CID length (1 byte)
                # - CID data (variable length)
                # - Block payload (remaining bytes)
                try:
                    # First byte is CID length
                    cid_length = block_data[0]

                    # Next bytes are the CID
                    cid_data = block_data[1 : cid_length + 1]

                    # Rest is the payload
                    payload = block_data[cid_length + 1 :]

                    # Inside the parse method of SimpleCARParser, modify the part where we store blocks:

                    # Store with CID as key
                    cid_hex = cid_data.hex()
                    self.blocks[cid_hex] = payload

                    # Then immediately after storing, try to decode and log the content:
                    try:
                        decoded = cbor2.loads(payload)
                        # If it's a post, log a snippet of the text
                        if isinstance(decoded, dict):
                            if (
                                "$type" in decoded
                                and decoded["$type"] == "app.bsky.feed.post"
                            ):
                                text = decoded.get("text", "")
                                if text:
                                    # Log the first 100 characters of the post text
                                    snippet = text[:100] + (
                                        "..." if len(text) > 100 else ""
                                    )
                                    logging.info(
                                        f'📄 Found post in block {cid_hex[:8]}: "{snippet}"'
                                    )
                    except Exception:
                        # Not CBOR or decode failed, just continue
                        pass
                except Exception as e:
                    logging.error(f"Error parsing block structure: {e}")
                    # Try an alternative approach - assume the entire block is the payload
                    # This is a fallback if the CID structure is different than expected
                    alt_cid_hex = f"alt_{len(self.blocks)}"
                    self.blocks[alt_cid_hex] = block_data
                    logging.warning(f"Using alternative CID {alt_cid_hex} for block")

        except Exception as e:
            logging.error(f"Error parsing CAR data: {e}")

        logging.info(f"Parsed {len(self.blocks)} blocks from CAR data")
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

    def extract_post_content(self, record):
        """Extract and format the content of a post record"""
        try:
            post_text = None

            # Handle different record structures we might encounter
            if isinstance(record, dict):
                # Direct record structure
                if "text" in record:
                    post_text = record["text"]
                # Nested record structure
                elif "record" in record and isinstance(record["record"], dict):
                    post_text = record["record"].get("text")

            if post_text:
                # Format any additional data we might want to show
                creator = record.get("repo", "Unknown User")

                # Get language if available
                langs = None
                if "langs" in record:
                    langs = record["langs"]
                elif "record" in record and isinstance(record["record"], dict):
                    langs = record["record"].get("langs")

                # Get created at timestamp
                created_at = None
                if "createdAt" in record:
                    created_at = record["createdAt"]
                elif "record" in record and isinstance(record["record"], dict):
                    created_at = record["record"].get("createdAt")

                # Format the output
                output = f'POST from {creator}: "{post_text}"'

                # Add additional metadata if available
                if langs:
                    output += f" [Language: {langs}]"
                if created_at:
                    output += f" [Posted at: {created_at}]"

                return output
            return "Post content could not be extracted"
        except Exception as e:
            logging.error(f"Error extracting post content: {e}")
            return "Error extracting post content"

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

                    # Use the new parsing function
                    parsed_data = self.parse_firehose_message(message)

                    if parsed_data:
                        header = parsed_data["header"]
                        commit = parsed_data["commit"]
                        blocks = parsed_data["blocks"]

                        # Extract repo and sequence for logging
                        repo = commit.get("repo", "")
                        seq = commit.get("seq", 0)
                        logging.info(f"Processing commit from repo {repo} seq {seq}")

                        # Process operations
                        for op in commit.get("ops", []):
                            action = op.get("action", "")
                            path = op.get("path", "")
                            logging.info(f" - {action} {path}")

                            # Check if we have the block data for this operation
                            if "cid" in op and isinstance(op["cid"], cbor2.CBORTag):
                                cid_hex = (
                                    op["cid"].value.hex()
                                    if hasattr(op["cid"].value, "hex")
                                    else None
                                )
                                if cid_hex and cid_hex in blocks:
                                    # We found the block data
                                    block_data = blocks[cid_hex]
                                    # We found the block data
                                    try:
                                        # Try to decode as CBOR
                                        record = cbor2.loads(block_data)

                                        # If this is a post, extract and display the content
                                        if "app.bsky.feed.post" in path:
                                            post_content = self.extract_post_content(
                                                {"repo": repo, "record": record}
                                            )
                                            logging.info(f"📝 {post_content}")

                                            # Also log any images or other media if present
                                            if (
                                                isinstance(record, dict)
                                                and "embed" in record
                                            ):
                                                embed_type = record["embed"].get(
                                                    "$type", "unknown embed"
                                                )
                                                logging.info(
                                                    f"   📎 Has embed: {embed_type}"
                                                )

                                                # If it's an image embed, show more details
                                                if (
                                                    embed_type
                                                    == "app.bsky.embed.images"
                                                ):
                                                    images = record["embed"].get(
                                                        "images", []
                                                    )
                                                    for i, img in enumerate(images):
                                                        alt = img.get(
                                                            "alt", "No description"
                                                        )
                                                        logging.info(
                                                            f"   🖼️ Image {i+1}: {alt}"
                                                        )
                                        else:
                                            # For non-post records, use the existing pretty print
                                            record_type = (
                                                path.split("/")[-1]
                                                if "/" in path
                                                else path
                                            )
                                            self.pretty_print_record(
                                                record_type,
                                                {"repo": repo, "record": record},
                                            )
                                    except Exception as e:
                                        logging.error(
                                            f"Failed to decode block data: {e}"
                                        )

                        result = {
                            "header": {"type": header.get("t"), "op": header.get("op")},
                            "commit": self.clean_commit_data(commit),
                            "records": {
                                # Convert blocks to a more friendly format
                                path: self.decode_record_cbor(
                                    blocks[op.get("cid").value.hex()]
                                )
                                for op in commit.get("ops", [])
                                if op.get("action") in ["create", "update"]
                                and "cid" in op
                                and isinstance(op["cid"], cbor2.CBORTag)
                                and hasattr(op["cid"].value, "hex")
                                and op["cid"].value.hex() in blocks
                            },
                        }

                        # Serialize with custom encoder
                        json_value = json.dumps(result, cls=CBORTagEncoder)

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

    def parse_firehose_message(self, message):
        """Parse a message from the Bluesky firehose"""
        try:
            # Log the raw bytes for debugging
            logging.debug(f"Raw message prefix: {message[:50].hex()}")

            # Step 1: Parse the header CBOR
            header = cbor2.loads(message)
            header_bytes = cbor2.dumps(header)

            # Step 2: Move to the next segment
            remainder = message[len(header_bytes) :]

            # Step 3: Parse the commit CBOR
            commit = cbor2.loads(remainder)
            commit_bytes = cbor2.dumps(commit)

            # Step 4: The rest should be CAR data
            car_data = remainder[len(commit_bytes) :]

            # Step 5: Process car blocks
            blocks = {}
            if len(car_data) > 0:
                blocks = self.car_parser.parse(car_data)

                # Check if we got any blocks
                if not blocks and len(car_data) > 0:
                    # If no blocks parsed but we have data, try direct CBOR decode
                    try:
                        direct_data = cbor2.loads(car_data)
                        if isinstance(direct_data, dict):
                            # Use a fake CID if it worked
                            blocks["direct_cbor"] = direct_data
                    except Exception:
                        pass

            return {"header": header, "commit": commit, "blocks": blocks}
        except Exception as e:
            logging.error(f"Failed to parse firehose message: {e}")
            # Dump the raw bytes to a file for further inspection
            with open("failed_message.bin", "wb") as f:
                f.write(message)
            return None

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
