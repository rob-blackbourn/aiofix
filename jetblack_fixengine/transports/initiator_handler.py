"""The Initiator base class"""

from abc import ABCMeta, abstractmethod
import asyncio
from datetime import datetime, time, tzinfo, timezone
import logging
from typing import Awaitable, Callable, Mapping, Any, Optional, Tuple, cast

from jetblack_fixparser.fix_message import FixMessageFactory, FixMessage
from jetblack_fixparser.meta_data import ProtocolMetaData
from ..types import Store, Event
from ..utils.date_utils import wait_for_time_period
from ..utils.json_utils import dict_to_json

from .initiator_state import (
    AsyncAdminStateMachine,
    AdminState,
    AdminEvent,
    AdminEventType
)

LOGGER = logging.getLogger(__name__)

STATE_DISCONNECTED = 'disconnected'
STATE_CONNECTED = 'connected'
STATE_LOGGING_ON = 'logon.start'
STATE_LOGGED_ON = 'logon.ok'
STATE_LOGGING_OFF = 'logout.start'
STATE_LOGGED_OUT = 'logout.done'

EPOCH_UTC = datetime.fromtimestamp(0, timezone.utc)


class InitiatorHandler(metaclass=ABCMeta):
    """The base class for initiators"""

    def __init__(
            self,
            protocol: ProtocolMetaData,
            sender_comp_id: str,
            target_comp_id: str,
            store: Store,
            heartbeat_timeout: int,
            cancellation_event: asyncio.Event,
            *,
            heartbeat_threshold: int = 1,
            logon_time_range: Optional[Tuple[time, time]] = None,
            tz: Optional[tzinfo] = None
    ) -> None:
        self.heartbeat_timeout = heartbeat_timeout
        self.heartbeat_threshold = heartbeat_threshold
        self.cancellation_event = cancellation_event
        self.logon_time_range = logon_time_range
        self.tz = tz
        self.fix_message_factory = FixMessageFactory(
            protocol,
            sender_comp_id,
            target_comp_id
        )

        self._state = STATE_DISCONNECTED
        self._last_send_time_utc: datetime = EPOCH_UTC
        self._last_receive_time_utc: datetime = EPOCH_UTC
        self._store = store
        self._session = self._store.get_session(sender_comp_id, target_comp_id)
        self._send: Optional[Callable[[Event], Awaitable[None]]] = None
        self._receive: Optional[Callable[[], Awaitable[Event]]] = None
        self._state_machine = AsyncAdminStateMachine({
            (AdminState.START, AdminEventType.CONNECTED): self._send_logon,
            (AdminState.LOGON_EXPECTED, AdminEventType.LOGON_RECEIVED): self._logon_received,
            (AdminState.LOGON_ACCEPTED, AdminEventType.LOGON_ACK): self._acknowledge_logon,

            (AdminState.PENDING, AdminEventType.HEARTBEAT_RECEIVED): self._acknowledge_heartbeat,
            (AdminState.PENDING, AdminEventType.TEST_REQUEST_RECEIVED): self._send_test_request,
            (AdminState.PENDING, AdminEventType.RESEND_REQUEST_RECEIVED): self._send_sequence_reset,
            (AdminState.PENDING, AdminEventType.LOGOUT_RECEIVED): self._acknowledge_logout
        })

    async def _send_logon(
            self,
            _fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        self._state = STATE_LOGGING_ON
        await self.send_message(
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
        ok = await self.on_logon(fix_message.message if fix_message else None)
        return AdminEventType.LOGON_ACK if ok else AdminEventType.LOGON_NACK

    async def _acknowledge_logon(
            self,
            _fix_message: Optional[FixMessage]
    ) -> Optional[AdminEventType]:
        LOGGER.info('Logged on')
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
        await self.send_message(
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
        await self.send_message(
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
        await self.on_logout(fix_message.message if fix_message else None)
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

    async def _send_event(self, event: Event, send_time_utc: datetime) -> None:
        if self._send is None:
            raise ValueError('Not connected')
        await self._send(event)
        self._last_send_time_utc = send_time_utc

    async def send_message(
            self,
            msg_type: str,
            message: Optional[Mapping[str, Any]] = None
    ) -> None:
        send_time_utc = datetime.now(timezone.utc)
        msg_seq_num = await self._next_outgoing_seqnum()
        fix_message = self.fix_message_factory.create(
            msg_type,
            msg_seq_num,
            send_time_utc,
            message
        )
        LOGGER.info(
            'Sending [%s]: %s',
            fix_message.meta_data.msgcat,
            dict_to_json(fix_message.message)
        )
        event = {
            'type': 'fix',
            'message': fix_message.encode(regenerate_integrity=True)
        }
        await self._send_event(event, send_time_utc)

    async def logout(self) -> None:
        """Send a logout message.
        """
        await self.send_message('LOGOUT')

    async def heartbeat(self, test_req_id: Optional[str] = None) -> None:
        """Send a heartbeat message.

        Args:
            test_req_id (Optional[str], optional): An optional test req id.
                Defaults to None.
        """
        body_kwargs = {}
        if test_req_id:
            body_kwargs['TestReqID'] = test_req_id
        await self.send_message('HEARTBEAT', body_kwargs)

    @abstractmethod
    async def on_application_message(self, message: Mapping[str, Any]) -> bool:
        """Handle an application message.

        Args:
            message (Mapping[str, Any]): The application message sent by the
                acceptor.

        Raises:
            NotImplementedError: [description]

        Returns:
            bool: If true the base handler will not handle the message.
        """
        ...

    async def _handle_event(self, event: Event) -> bool:
        if event['type'] == 'fix':
            fix_message: FixMessage = event['fix_message']

            LOGGER.info(
                'Received [%s]: %s',
                fix_message.meta_data.msgcat,
                dict_to_json(fix_message.message)
            )

            await self._session.save_message(event['message'])

            msgcat = cast(str, fix_message.meta_data.msgcat)
            if msgcat == 'admin':
                event_type = AdminEvent.to_event_type(fix_message)
                await self._state_machine.handle_event(event_type, fix_message)
                status = self._state_machine != AdminState.STOP
            else:
                status = await self.on_application_message(fix_message.message)

            msg_seq_num: int = cast(int, fix_message.message['MsgSeqNum'])
            await self._set_incoming_seqnum(msg_seq_num)

            self._last_receive_time_utc = datetime.now(timezone.utc)

            return status

        elif event['type'] == 'error':
            LOGGER.warning('error')
            return False
        elif event['type'] == 'disconnect':
            return False
        else:
            return False

    async def _handle_heartbeat(self) -> float:
        now_utc = datetime.now(timezone.utc)
        seconds_since_last_send = (
            now_utc - self._last_send_time_utc
        ).total_seconds()
        if seconds_since_last_send >= self.heartbeat_timeout and self._state == STATE_LOGGED_ON:
            await self.heartbeat()
            seconds_since_last_send = 0

        return self.heartbeat_timeout - seconds_since_last_send

    async def _handle_timeout(self) -> None:
        if not self._state == STATE_LOGGED_ON:
            return

        now_utc = datetime.now(timezone.utc)
        seconds_since_last_receive = (
            now_utc - self._last_receive_time_utc).total_seconds()
        if seconds_since_last_receive - self.heartbeat_timeout > self.heartbeat_threshold:
            await self.send_message(
                'TEST_REQUEST',
                {
                    'TestReqID': 'SOMETHING'
                }
            )

    @abstractmethod
    async def on_logon(self, message: Optional[Mapping[str, Any]]) -> bool:
        ...

    @abstractmethod
    async def on_logout(self, message: Optional[Mapping[str, Any]]) -> None:
        ...

    async def _wait_till_logon_time(self) -> Optional[datetime]:
        if self.logon_time_range:
            start_time, end_time = self.logon_time_range
            LOGGER.info('Logon from %s to %s', start_time, end_time)
            end_datetime = await wait_for_time_period(
                datetime.now(tz=self.tz),
                start_time,
                end_time,
                cancellation_event=self.cancellation_event
            )
            return end_datetime

        return None

    async def __call__(
            self,
            send: Callable[[Event], Awaitable[None]],
            receive: Callable[[], Awaitable[Event]]
    ) -> None:
        self._send, self._receive = send, receive

        event = await receive()

        if event['type'] == 'connected':
            LOGGER.info('connected')
            self._state = STATE_CONNECTED
            await self._state_machine.handle_event(AdminEventType.CONNECTED, None)

            ok = True
            while ok:
                try:
                    timeout = await self._handle_heartbeat()

                    event = await asyncio.wait_for(
                        receive(),
                        timeout=timeout
                    )

                    if event['type'] == 'fix':
                        event['fix_message'] = self.fix_message_factory.decode(
                            event['message']
                        )

                    ok = await self._handle_event(event)
                except asyncio.TimeoutError:
                    await self._handle_timeout()
        else:
            raise RuntimeError('Failed to connect')

        LOGGER.info('disconnected')
        self._state = STATE_DISCONNECTED
