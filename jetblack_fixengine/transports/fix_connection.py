"""Async FIX message reader"""

import asyncio
from asyncio import StreamWriter
import logging
from ssl import SSLContext
from typing import AsyncIterator, Awaitable, Callable, Optional, Tuple

from jetblack_fixparser import FixMessage
from jetblack_fixparser.meta_data import ProtocolMetaData

from .fix_reader import FixReader as FixParser
from .async_fix_reader import async_fix_reader

FixReader = Callable[[], Awaitable[Tuple[FixMessage, bytes]]]
FixWriter = Callable[[FixMessage], Awaitable[None]]

LOGGER = logging.getLogger(__name__)


def _create_fix_streams(
        reader: AsyncIterator[bytes],
        writer: StreamWriter,
        decode: Callable[[bytes], FixMessage],
        encode: Callable[[FixMessage], bytes]
) -> Tuple[FixReader, FixWriter]:
    stream_iter = reader.__aiter__()

    async def read() -> Tuple[FixMessage, bytes]:
        buf = await stream_iter.__anext__()
        message = decode(buf)
        return message, buf

    async def write(message: FixMessage) -> None:
        buf = encode(message)
        writer.write(buf)
        await writer.drain()

    return read, write


async def open_fix_connection(
        host: str,
        port: int,
        protocol: ProtocolMetaData,
        *,
        ssl: Optional[SSLContext] = None,
        blksiz: int = 1024
) -> Tuple[FixReader, FixWriter]:
    reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
    parser = FixParser()
    buffered_reader = async_fix_reader(parser, reader, blksiz)

    return _create_fix_streams(
        buffered_reader,
        writer,
        lambda buf: FixMessage.decode(protocol, buf),
        lambda message: message.encode()
    )
