"""Initiator transport"""

import asyncio
import logging
from ssl import SSLContext
from typing import Optional, Callable, Type

from jetblack_fixparser.meta_data import ProtocolMetaData
from jetblack_fixparser.fix_message import SOH

from ..types import Handler, Store
from ..utils.cancellation import register_cancellation_event

from .fix_transport import fix_stream_processor
from .fix_read_buffer import FixReadBuffer
from .fix_reader_async import fix_read_async
from .initiator_handler import InitiatorHandler

LOGGER = logging.getLogger(__name__)

InitiatorFactory = Callable[
    [ProtocolMetaData, str, str, Store, int, asyncio.Event],
    Handler
]


async def initiate(
        host: str,
        port: int,
        handler: Handler,
        cancellation_event: asyncio.Event,
        *,
        ssl: Optional[SSLContext] = None,
        shutdown_timeout: float = 10.0,
        sep: bytes = SOH,
        convert_sep_to_soh_for_checksum: bool = False,
        validate: bool = True
) -> None:
    LOGGER.info(
        'connecting to %s:%s%s',
        host,
        port,
        " over ssl" if ssl else ""
    )

    reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
    read_buffer = FixReadBuffer(sep, convert_sep_to_soh_for_checksum, validate)
    buffered_reader = fix_read_async(read_buffer, reader, 1024)
    await fix_stream_processor(
        handler,
        shutdown_timeout,
        buffered_reader,
        writer,
        cancellation_event
    )

    LOGGER.info(
        'disconnected from %s:%s%s',
        host,
        port,
        " over ssl" if ssl else ""
    )


def create_initiator(
        klass: Type[InitiatorHandler],
        protocol: ProtocolMetaData,
        sender_comp_id: str,
        target_comp_id: str,
        store: Store,
        heartbeat_timeout: int,
        cancellation_event: asyncio.Event,
        *,
        heartbeat_threshold: int = 1
) -> InitiatorHandler:
    handler = klass(
        protocol,
        sender_comp_id,
        target_comp_id,
        store,
        heartbeat_timeout,
        cancellation_event,
        heartbeat_threshold=heartbeat_threshold
    )
    return handler


def start_initiator(
        klass: Type[InitiatorHandler],
        host: str,
        port: int,
        protocol: ProtocolMetaData,
        sender_comp_id: str,
        target_comp_id: str,
        store: Store,
        heartbeat_timeout: int,
        *,
        ssl: Optional[SSLContext] = None,
        shutdown_timeout: float = 10.0,
        heartbeat_threshold: int = 1
) -> None:
    cancellation_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    register_cancellation_event(cancellation_event, loop)

    handler = create_initiator(
        klass,
        protocol,
        sender_comp_id,
        target_comp_id,
        store,
        heartbeat_timeout,
        cancellation_event,
        heartbeat_threshold=heartbeat_threshold
    )

    loop.run_until_complete(
        initiate(
            host,
            port,
            handler,
            cancellation_event,
            ssl=ssl,
            shutdown_timeout=shutdown_timeout
        )
    )
