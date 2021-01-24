"""An async fix reader"""

from asyncio import StreamReader
from typing import AsyncIterator, cast

from .fix_events import FixReadEventType, FixReadDataReady
from .fix_reader import FixReader


async def async_fix_reader(
        fix_reader: FixReader,
        stream_reader: StreamReader,
        blksiz: int
) -> AsyncIterator[bytes]:
    """An async reader for a stream of FIX messages.

    Args:
        fix_reader (FixReader): The FIX read buffer.
        stream_reader (StreamReader): A stream reader.
        blksiz (int): The read block size.

    Raises:
        Exception: If the reader is in an invalid state.

    Yields:
        Iterator[AsyncIterator[bytes]]: A bytes buffer containing a raw FIX
            message.
    """
    done = False
    while not done:
        fix_event = fix_reader.next_event()
        if fix_event.event_type == FixReadEventType.EOF:
            done = True
        elif fix_event.event_type == FixReadEventType.NEEDS_MORE_DATA:
            buf = await stream_reader.read(blksiz)
            fix_reader.receive(buf)
        elif fix_event.event_type == FixReadEventType.DATA_READY:
            data_ready = cast(FixReadDataReady, fix_event)
            yield data_ready.data
        else:
            raise Exception('Invalid state')
