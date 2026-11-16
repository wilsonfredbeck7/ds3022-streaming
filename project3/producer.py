"""Bluesky posts -> Kafka topic "bluesky_posts".

  python producer.py        # PUSH: new posts from the Bluesky Jetstream WebSocket

Adapted from the class quakes_producer.py (Professor Magee's stream-seismic.py, learn-kafka).
Connector pattern from the Session 1 worksheet, Appendix A. Jetstream URL from probe.py.
"""
import asyncio
import json
import logging
import sys

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_posts"  # part 1: the name on my card
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
SUBSCRIBE = None  # Bluesky Jetstream: no subscribe message

count = 0


def send(producer, message):
    """Part 2: one Kafka record per message, keyed as on my card."""
    global count
    key = message["data"].get("did")  # .get(): no crash if missing
    if key is None:
        logging.warning("No did: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))
    count += 1
    if count % 1000 == 0:  # part 4: one line every 1,000 messages
        logging.info("%d messages sent to %s", count, TOPIC)


async def live(producer):
    """Part 3: PUSH over a WebSocket, with a reconnect loop."""
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            if SUBSCRIBE:
                await ws.write_message(json.dumps(SUBSCRIBE))
            while True:
                text = await ws.read_message()
                if text is None:
                    break  # the server closed the connection
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                except (ValueError, KeyError, TypeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass