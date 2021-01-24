"""Initiator"""

import asyncio
from asyncio import Queue, Task
from datetime import datetime, timezone
import logging
from typing import (
    Any,
    Literal,
    Mapping,
    Optional,
    TypedDict,
    Union,
    TYPE_CHECKING,
    cast
)

from jetblack_fixparser.fix_message import FixMessageFactory, FixMessage
from jetblack_fixparser.meta_data import ProtocolMetaData
from ..types import Store
from ..utils.async_iter import aiter_adapter
from ..utils.cancellation import cancel_await
from ..utils.json_utils import dict_to_json

from .initiator_state import (
    AsyncAdminStateMachine,
    AdminState,
    AdminEvent,
    AdminEventType
)
from .fix_connection import (
    FixReader,
    FixWriter
)

if TYPE_CHECKING:
    # pylint: disable=ungrouped-imports
    from asyncio import Future
    from typing import Set


LOGGER = logging.getLogger(__name__)

EPOCH_UTC = datetime.fromtimestamp(0, timezone.utc)


class FixReceivedEvent(TypedDict):
    type: Literal['fix.received']
    msg_type: str
    message: Mapping[str, Any]


class FixSendEvent(TypedDict):
    type: Literal['fix.send']
    msg_type: str
    message: Mapping[str, Any]


class LogonRequestEvent(TypedDict):
    type: Literal['logon.request']
    message: Mapping[str, Any]


class LogonResponseEvent(TypedDict):
    type: Literal['logon.response']
    is_allowed: bool


class LogoutReceivedEvent(TypedDict):
    type: Literal['logout.received']
    message: Mapping[str, Any]


OutgoingEvent = Union[FixReceivedEvent, LogonRequestEvent, LogoutReceivedEvent]
IncomingEvent = Union[FixSendEvent, LogonResponseEvent]


class Initiator:

    def __init__(
            self,
            read: FixReader,
            write: FixWriter,
            protocol: ProtocolMetaData,
            sender_comp_id: str,
            target_comp_id: str,
            store: Store,
            heartbeat_timeout: int,
            cancellation_event: asyncio.Event,
            *,
            heartbeat_threshold: int = 1
    ) -> None:
        self._read = read
        self._write = write
        self.heartbeat_timeout = heartbeat_timeout
        self.heartbeat_threshold = heartbeat_threshold
        self._cancellation_event = cancellation_event
        self._fix_message_factory = FixMessageFactory(
            protocol,
            sender_comp_id,
            target_comp_id
        )

        self._last_send_time: datetime = EPOCH_UTC
        self._last_receive_time: datetime = EPOCH_UTC
        self._store = store
        self._session = self._store.get_session(sender_comp_id, target_comp_id)
        self._state_machine = AsyncAdminStateMachine({
            (AdminState.START, AdminEventType.CONNECTED): self._send_logon,
            (AdminState.LOGON_EXPECTED, AdminEventType.LOGON_RECEIVED): self._logon_received,
            (AdminState.PENDING, AdminEventType.HEARTBEAT_RECEIVED): self._acknowledge_heartbeat,
            (AdminState.PENDING, AdminEventType.TEST_REQUEST_RECEIVED): self._send_test_request,
            (AdminState.PENDING, AdminEventType.RESEND_REQUEST_RECEIVED): self._send_sequence_reset,
            (AdminState.PENDING, AdminEventType.LOGOUT_RECEIVED): self._acknowledge_logout
        })
        self._events_in: "Queue[IncomingEvent]" = Queue()
        self._events_out: "Queue[OutgoingEvent]" = Queue()

    async def _send_logon(
            self,
            _fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        await self._send_message(
            'LOGON',
            {
                'EncryptMethod': 'NONE',
                'HeartBtInt': self.heartbeat_timeout
            }
        )
        return AdminEventType.LOGON_SENT

    async def _logon_received(
            self,
            fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        assert fix_message is not None
        event: LogonRequestEvent = {
            'type': 'logon.request',
            'message': fix_message.message
        }
        await self._events_out.put(event)
        return None

    async def _acknowledge_heartbeat(
            self,
            _fix_mesage: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        return AdminEventType.HEARTBEAT_ACK

    async def _send_test_request(
            self,
            fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        if fix_message is None:
            raise ValueError()
        await self._send_message(
            'TEST_REQUEST',
            {
                'TestReqID': fix_message.message['TestReqID']
            }
        )
        return AdminEventType.TEST_REQUEST_SENT

    async def _send_sequence_reset(
            self,
            _fix_mesage: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        seqnum = await self._session.get_outgoing_seqnum() + 2
        await self._send_message(
            'SEQUENCE_RESET',
            {
                'GapFillFlag': False,
                'NewSeqNo': seqnum
            }
        )
        return AdminEventType.SEQUENCE_RESET_SENT

    async def _acknowledge_logout(
            self,
            fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        assert fix_message is not None
        event: LogoutReceivedEvent = {
            'type': 'logout.received',
            'message': fix_message.message
        }
        await self._events_out.put(event)
        return AdminEventType.LOGOUT_ACK

    async def _next_outgoing_seqnum(self) -> int:
        seqnum = await self._session.get_outgoing_seqnum()
        seqnum += 1
        await self._session.set_outgoing_seqnum(seqnum)
        return seqnum

    async def _set_seqnums(self, outgoing_seqnum: int, incoming_seqnum: int) -> None:
        await self._session.set_seqnums(outgoing_seqnum, incoming_seqnum)

    async def _set_incoming_seqnum(self, seqnum: int) -> None:
        await self._session.set_incoming_seqnum(seqnum)

    async def _send_message(
            self,
            msg_type: str,
            message: Optional[Mapping[str, Any]] = None
    ) -> None:
        timestamp = datetime.now(timezone.utc)
        msg_seq_num = await self._next_outgoing_seqnum()
        fix_message = self._fix_message_factory.create(
            msg_type,
            msg_seq_num,
            timestamp,
            message
        )
        LOGGER.info(
            'Sending [%s]: %s',
            fix_message.meta_data.msgcat,
            dict_to_json(fix_message.message)
        )
        self._last_send_time = timestamp
        await self._write(fix_message)

    async def _logout(self) -> None:
        """Send a logout message.
        """
        await self._send_message('LOGOUT')

    async def _handle_message(self, fix_message: FixMessage) -> None:
        LOGGER.info(
            'Received [%s]: %s',
            fix_message.meta_data.msgcat,
            dict_to_json(fix_message.message)
        )

        if fix_message.meta_data.msgcat == 'admin':
            event_type = AdminEvent.to_event_type(fix_message)
            await self._state_machine.handle_event(event_type, fix_message)
        else:
            event: FixReceivedEvent = {
                'type': 'fix.received',
                'msg_type': fix_message.meta_data.msgtype.decode('ascii'),
                'message': fix_message.message
            }
            await self._events_out.put(event)

        # TODO: Should we do this earlier?
        msg_seq_num: int = fix_message.message['MsgSeqNum']
        await self._set_incoming_seqnum(msg_seq_num)
        self._last_receive_time = datetime.now(timezone.utc)

    async def _handle_heartbeat(self) -> float:
        timestamp = datetime.now(timezone.utc)
        elapsed = (timestamp - self._last_send_time).total_seconds()
        if (
                elapsed >= self.heartbeat_timeout and
                self._state_machine.state == AdminState.PENDING
        ):
            await self._send_message('HEARTBEAT')
            elapsed = 0

        return self.heartbeat_timeout - elapsed

    async def _handle_timeout(self) -> None:
        if self._state_machine.state != AdminState.PENDING:
            return

        timestamp = datetime.now(timezone.utc)
        elapsed = (
            timestamp - self._last_receive_time
        ).total_seconds()
        if elapsed - self.heartbeat_timeout > self.heartbeat_threshold:
            await self._send_message(
                'TEST_REQUEST',
                {
                    'TestReqID': 'SOMETHING'
                }
            )

    async def _process_fix_messages(self) -> None:

        await self._state_machine.handle_event(AdminEventType.CONNECTED, None)

        while not (
                self._cancellation_event.is_set() or
                self._state_machine.state == AdminState.STOP
        ):
            try:
                timeout = await self._handle_heartbeat()

                fix_message, buf = await asyncio.wait_for(
                    self._read(),
                    timeout=timeout
                )

                await self._session.save_message(buf)
                await self._handle_message(fix_message)
            except asyncio.TimeoutError:
                await self._handle_timeout()

    async def _handle_logon_response(self, event: LogonResponseEvent) -> None:
        is_allowed = event['is_allowed']
        event_type = AdminEventType.LOGON_ACK if is_allowed else AdminEventType.LOGON_NACK
        LOGGER.debug("logon response: %s", event_type)
        await self._state_machine.handle_event(event_type, None)

    async def _handle_fix_send(self, event: FixSendEvent) -> None:
        msg_type = event['msg_type']
        message = event['message']
        await self._send_message(msg_type, message)

    async def _process_events_in(self):
        async for event in aiter_adapter(self._events_in.get, self._cancellation_event):
            if event['type'] == 'logon.response':
                await self._handle_logon_response(event)
            if event['type'] == 'fix.send':
                await self._handle_fix_send(event)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Mapping[str, Any]:
        return await self._events_out.get()

    async def start(self) -> None:
        fix_task = asyncio.create_task(
            self._process_fix_messages(),
            name="process-fix-messages"
        )
        event_task = asyncio.create_task(
            self._process_events_in(),
            name="process-events-in"
        )
        pending: "Set[Future[Any]]" = {
            fix_task,
            event_task
        }
        _done, pending = await asyncio.wait(pending)
        for task in pending:
            await cancel_await(cast(Task, task))

    async def send_event(self, event: IncomingEvent):
        await self._events_in.put(event)
