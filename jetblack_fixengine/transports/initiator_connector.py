"""Initiator"""

import asyncio
from asyncio import Task
import logging
from ssl import SSLContext
from typing import (
    Optional,
    TYPE_CHECKING
)

from jetblack_fixparser.meta_data import ProtocolMetaData
from ..types import Store

from .fix_connection import (
    open_fix_connection
)
from .initiator import Initiator

if TYPE_CHECKING:
    # pylint: disable=ungrouped-imports
    from asyncio import Future
    from typing import Set


LOGGER = logging.getLogger(__name__)


class InitiatorConnector:

    def __init__(
            self,
            host: str,
            port: int,
            protocol: ProtocolMetaData,
            sender_comp_id: str,
            target_comp_id: str,
            store: Store,
            heartbeat_timeout: int,
            cancellation_event: asyncio.Event,
            *,
            heartbeat_threshold: int = 1,
            ssl: Optional[SSLContext] = None,
            blksiz: int = 1024
    ) -> None:
        self.host = host
        self.port = port
        self.protocol = protocol
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.store = store
        self.heartbeat_timeout = heartbeat_timeout
        self.cancellation_event = cancellation_event
        self.heartbeat_threshold = heartbeat_threshold
        self.ssl = ssl
        self.blksiz = blksiz
        self._initiator: Optional[Initiator] = None
        self._initiator_task: Optional[Task] = None

    async def open(self) -> Initiator:
        reader, writer = await open_fix_connection(
            self.host,
            self.port,
            self.protocol,
            ssl=self.ssl,
            blksiz=self.blksiz
        )
        self._initiator = Initiator(
            reader,
            writer,
            self.protocol,
            self.sender_comp_id,
            self.target_comp_id,
            self.store,
            self.heartbeat_timeout,
            self.cancellation_event,
            heartbeat_threshold=self.heartbeat_threshold
        )
        self._initiator_task = asyncio.create_task(self._initiator.start())
        return self._initiator

    async def close(self) -> None:
        assert self._initiator is not None, "initiator not created"
        assert self._initiator_task, "initiator not started"
        self._initiator_task.cancel()
        await self._initiator_task

    async def __aenter__(self) -> Initiator:
        return await self.open()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
