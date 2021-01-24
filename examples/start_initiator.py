"""A simple Initiator"""

import asyncio
from asyncio import Event
import logging
import os.path
from typing import Mapping, Any, Optional

from jetblack_fixparser.loader import load_yaml_protocol
from jetblack_fixengine.persistence import FileStore
# from jetblack_fixengine.transports import start_initiator, InitiatorHandler
from jetblack_fixengine.transports import InitiatorConnector, Initiator
from jetblack_fixengine.utils.cancellation import register_cancellation_event


logging.basicConfig(level=logging.DEBUG)

LOGGER = logging.getLogger(__name__)

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
etc = os.path.join(root, 'etc')

STORE = FileStore(os.path.join(root, 'store'))
HOST = '127.0.0.1'
PORT = 9801
SENDER_COMP_ID = 'INITIATOR1'
TARGET_COMP_ID = 'ACCEPTOR'
HEARTBEAT_TIMEOUT = 30
PROTOCOL = load_yaml_protocol('etc/FIX44.yaml')


async def main():
    loop = asyncio.get_event_loop()
    cancellation_event = Event()
    register_cancellation_event(
        cancellation_event,
        loop
    )
    connector = InitiatorConnector(
        HOST,
        PORT,
        PROTOCOL,
        SENDER_COMP_ID,
        TARGET_COMP_ID,
        STORE,
        HEARTBEAT_TIMEOUT,
        cancellation_event
    )
    async with connector as initiator:
        async for event in initiator:
            if event['type'] == 'logon.request':
                LOGGER.info('login requested')
                response: LogonResponseEvent = {
                    'type': 'logon.response',
                    'is_allowed': True
                }
                await initiator.send_event(response)

if __name__ == "__main__":
    asyncio.run(main())
