"""Initiator state management"""

from enum import Enum, auto
from typing import Awaitable, Callable, Mapping, Optional, Tuple

from jetblack_fixparser.fix_message import FixMessage


class AdminState(Enum):
    """The state for initiator admin messages"""
    START = auto()
    LOGON_REQUESTED = auto()
    LOGON_EXPECTED = auto()
    LOGON_ACCEPTED = auto()
    LOGON_REJECTED = auto()
    PENDING = auto()
    ACKNOWLEDGE_HEARTBEAT = auto()
    TEST_REQUEST_REQUESTED = auto()
    SEQUENCE_RESET_REQUESTED = auto()
    SET_INCOMING_SEQNUM = auto()
    ACKNOWLEDGE_LOGOUT = auto()
    STOP = auto()


class AdminEventType(Enum):
    CONNECTED = auto()
    LOGON_RECEIVED = auto()
    LOGON_SENT = auto()
    LOGON_ACCEPTED = auto()
    LOGON_REJECTED = auto()
    LOGON_ACKNOWLEDGED = auto()
    REJECT_ACKNOWLEDGED = auto()
    REJECT_RECEIVED = auto()
    HEARTBEAT_RECEIVED = auto()
    HEARTBEAT_ACKNOWLEDGED = auto()
    TEST_REQUEST_RECEIVED = auto()
    TEST_REQUEST_SENT = auto()
    RESEND_REQUEST_RECEIVED = auto()
    SEQUENCE_RESET_SENT = auto()
    SEQUENCE_RESET_RECEIVED = auto()
    INCOMING_SEQNUM_SET = auto()
    LOGOUT_RECEIVED = auto()
    LOGOUT_ACKNOWLEDGED = auto()


class AdminEvent:

    ADMIN_MSGTYPE_EVENTS = {
        'LOGON': AdminEventType.LOGON_RECEIVED,
        'HEARTBEAT': AdminEventType.HEARTBEAT_RECEIVED,
        'TEST_REQUEST': AdminEventType.TEST_REQUEST_RECEIVED,
        'RESEND_REQUEST': AdminEventType.RESEND_REQUEST_RECEIVED,
        'SEQUENCE_RESET': AdminEventType.SEQUENCE_RESET_RECEIVED,
        'LOGOUT': AdminEventType.LOGOUT_RECEIVED
        # TODO: These messages are not handled.
        # REJECT
        # XML_MESSAGE
    }

    def __init__(
            self,
            event_type: AdminEventType,
            fix_message: FixMessage
    ) -> None:
        self.event_type = event_type
        self.fix_message = fix_message

    @classmethod
    def to_event_type(cls, fix_message: FixMessage):
        return cls.ADMIN_MSGTYPE_EVENTS[fix_message['MsgType']]


class AdminStateMachine:
    """State machine for the initiator admin messages"""

    TRANSITIONS: Mapping[Tuple[AdminState, AdminEventType], AdminState] = {
        (AdminState.START, AdminEventType.CONNECTED): AdminState.LOGON_REQUESTED,
        (AdminState.LOGON_REQUESTED, AdminEventType.LOGON_SENT): AdminState.LOGON_EXPECTED,
        (AdminState.LOGON_EXPECTED, AdminEventType.LOGON_RECEIVED): AdminState.LOGON_ACCEPTED,
        (AdminState.LOGON_EXPECTED, AdminEventType.REJECT_RECEIVED): AdminState.LOGON_REJECTED,
        (AdminState.LOGON_ACCEPTED, AdminEventType.LOGON_ACKNOWLEDGED): AdminState.PENDING,
        (AdminState.LOGON_REJECTED, AdminEventType.REJECT_ACKNOWLEDGED): AdminState.STOP,

        # Acceptor heartbeet
        (AdminState.PENDING, AdminEventType.HEARTBEAT_RECEIVED): AdminState.ACKNOWLEDGE_HEARTBEAT,
        (AdminState.ACKNOWLEDGE_HEARTBEAT, AdminEventType.HEARTBEAT_ACKNOWLEDGED): AdminState.PENDING,

        # Test Request
        (AdminState.PENDING, AdminEventType.TEST_REQUEST_RECEIVED): AdminState.TEST_REQUEST_REQUESTED,
        (AdminState.TEST_REQUEST_REQUESTED, AdminEventType.TEST_REQUEST_SENT): AdminState.PENDING,

        # Resend Request
        (AdminState.PENDING, AdminEventType.RESEND_REQUEST_RECEIVED): AdminState.SEQUENCE_RESET_REQUESTED,
        (AdminState.SEQUENCE_RESET_REQUESTED, AdminEventType.SEQUENCE_RESET_SENT): AdminState.PENDING,

        # Sequence Reset
        (AdminState.PENDING, AdminEventType.SEQUENCE_RESET_RECEIVED): AdminState.SET_INCOMING_SEQNUM,
        (AdminState.SET_INCOMING_SEQNUM, AdminEventType.INCOMING_SEQNUM_SET): AdminState.PENDING,

        # Logout
        (AdminState.PENDING, AdminEventType.LOGOUT_RECEIVED): AdminState.ACKNOWLEDGE_LOGOUT,
        (AdminState.ACKNOWLEDGE_LOGOUT, AdminEventType.LOGOUT_ACKNOWLEDGED): AdminState.STOP
    }

    def __init__(self):
        self.state = AdminState.START

    def next_event(self, event: AdminEventType) -> AdminState:
        self.state = self.TRANSITIONS[(self.state, event)]
        return self.state

    def __str__(self) -> str:
        return f"InitiatorStateMachine: state={self.state}"

    __repr__ = __str__


Handler = Callable[[Optional[FixMessage]], Awaitable[Optional[AdminEventType]]]


class AsyncAdminStateMachine(AdminStateMachine):

    def __init__(
            self,
            state_handlers: Mapping[Tuple[AdminState, AdminEventType], Handler]
    ) -> None:
        super().__init__()
        self.state_handlers = state_handlers

    async def handle_event(
            self,
            event: Optional[AdminEventType],
            fix_message: Optional[FixMessage]
    ) -> AdminState:
        if event is None:
            return self.state
        if (self.state, event) not in self.state_handlers:
            self.next_event(event)
            return self.state
        else:
            handler = self.state_handlers[(self.state, event)]
            self.next_event(event)
            return await self.handle_event(await handler(fix_message), None)
