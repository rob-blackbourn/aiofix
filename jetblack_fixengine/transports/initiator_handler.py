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

    async def logon(self) -> None:
        """Send a logon message"""
        self._state = STATE_LOGGING_ON
        await self.send_message(
            'LOGON',
            {
                'EncryptMethod': 'NONE',
                'HeartBtInt': self.heartbeat_timeout
            }
        )

    async def logout(self) -> None:
        """Send a logout message.
        """
        self._state = STATE_LOGGING_OFF
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

    async def resend_request(self, begin_seqnum: int, end_seqnum: int = 0) -> None:
        """Send a resend request.

        Args:
            begin_seqnum (int): The begin seqnum
            end_seqnum (int, optional): An optional end seqnum. Defaults to 0.
        """
        await self.send_message(
            'RESEND_REQUEST',
            {
                'BeginSeqNo': begin_seqnum,
                'EndSeqNo': end_seqnum
            }
        )

    async def test_request(self, test_req_id: str) -> None:
        """Send a test request.

        Args:
            test_req_id (str): The test req id.
        """
        await self.send_message(
            'TEST_REQUEST',
            {
                'TestReqID': test_req_id
            }
        )

    async def sequence_reset(self, gap_fill: bool, new_seq_no: int) -> None:
        """Send a sequence reset.

        Args:
            gap_fill (bool): If true set the GapFillFlag.
            new_seq_no (int): The new seqnum.
        """
        await self.send_message(
            'SEQUENCE_RESET',
            {
                'GapFillFlag': gap_fill,
                'NewSeqNo': new_seq_no
            }
        )

    async def _on_admin_message(self, message: Mapping[str, Any]) -> bool:
        # Only handle if unhandled by the overrideing method.
        override_status = await self.on_admin_message(message)
        if override_status is not None:
            return override_status

        if message['MsgType'] == 'LOGON':
            await self.on_logon()
            self._state = STATE_LOGGED_ON
            return True
        elif message['MsgType'] == 'HEARTBEAT':
            return True
        elif message['MsgType'] == 'TEST_REQUEST':
            await self.test_request(message['TestReqID'])
            return True
        elif message['MsgType'] == 'RESEND_REQUEST':
            await self.sequence_reset(False, await self._session.get_outgoing_seqnum() + 2)
            return True
        elif message['MsgType'] == 'SEQUENCE_RESET':
            await self._set_incoming_seqnum(message['NewSeqNo'])
            return True
        elif message['MsgType'] == 'LOGOUT':
            self._state = STATE_LOGGED_OUT
            return False
        else:
            LOGGER.warning(
                'unhandled admin message type "%s".',
                message["MsgType"]
            )
            return True

    @abstractmethod
    async def on_admin_message(self, message: Mapping[str, Any]) -> Optional[bool]:
        """Handle an admin message

        Args:
            message (Mapping[str, Any]): The admin message that was sent by the
                acceptor.

        Raises:
            NotImplementedError: [description]

        Returns:
            Optional[bool]: If true the message will override the base handler.
        """
        ...

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
                status = await self._on_admin_message(fix_message.message)
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
            await self.test_request('TEST')

    @abstractmethod
    async def on_logon(self) -> None:
        """Called when a logon message is received.
        """
        ...

    @abstractmethod
    async def on_logout(self) -> None:
        """Called when a logout message is received.
        """
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

            end_datetime = await self._wait_till_logon_time()
            await self.logon()

            ok = True
            while ok:
                try:
                    if (
                            self._state == STATE_LOGGED_ON and
                            end_datetime and
                            datetime.now(tz=self.tz) >= end_datetime
                    ):
                        await self.logout()
                        await self.on_logout()

                    timeout = await self._handle_heartbeat()
                    event = await asyncio.wait_for(receive(), timeout=timeout)

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
