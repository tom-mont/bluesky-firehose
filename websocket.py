import asyncio
import websockets
import json
from typing import Callable, Any, Optional
import logging


class WebSocketClient:
    def __init__(
        self,
        uri: str,
        on_message: Callable[[str], Any],
        on_error: Optional[Callable[[Exception], Any]] = None,
        on_close: Optional[Callable[[], Any]] = None,
        reconnect_interval: int = 5,
    ):
        self.uri = uri
        self.on_message = on_message
        self.on_error = on_error or (lambda e: logging.error(f"WebSocket error: {e}"))
        self.on_close = on_close or (
            lambda: logging.info("WebSocket connection closed")
        )
        self.reconnect_interval = reconnect_interval
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
                self.on_error(e)
                await asyncio.sleep(self.reconnect_interval)

    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        try:
            async for message in self.websocket:
                try:
                    # Assuming JSON messages, modify as needed
                    data = json.loads(message)
                    await self.on_message(data)
                except json.JSONDecodeError as e:
                    # Handle raw message if not JSON
                    await self.on_message(message)
        except websockets.ConnectionClosed:
            self.on_close()

    async def start(self):
        """Start the WebSocket client"""
        self.running = True
        await self.connect()

    async def stop(self):
        """Stop the WebSocket client"""
        self.running = False
        if self.websocket:
            await self.websocket.close()

    async def send(self, message: str):
        """Send message to WebSocket server"""
        if self.websocket:
            await self.websocket.send(message)
