"""Initiator state management"""

from enum import Enum, auto
from typing import Awaitable, Callable, Mapping, Optional, Tuple


class AdminState(Enum):
    """The state for initiator admin messages"""
    START = auto()
    LOGON_REQUESTED = auto()
    LOGON_EXPECTED = auto()
    PENDING = auto()
    ACKNOWLEDGE_HEARTBEAT = auto()
    TEST_REQUEST_REQUESTED = auto()
    SEQUENCE_RESET_REQUESTED = auto()
    SET_INCOMING_SEQNUM = auto()
    ACKNOWLEDGE_LOGOUT = auto()
    STOP = auto()


class AdminEvent(Enum):
    CONNECTED = auto()
    LOGON_RECEIVED = auto()
    LOGON_SENT = auto()
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


ADMIN_MSGTYPE_EVENTS = {
    'LOGON': AdminEvent.LOGON_RECEIVED,
    'HEARTBEAT': AdminEvent.HEARTBEAT_RECEIVED,
    'TEST_REQUEST': AdminEvent.TEST_REQUEST_RECEIVED,
    'RESEND_REQUEST': AdminEvent.RESEND_REQUEST_RECEIVED,
    'SEQUENCE_RESET': AdminEvent.SEQUENCE_RESET_RECEIVED,
    'LOGOUT': AdminEvent.LOGOUT_RECEIVED
}


class AdminStateMachine:
    """State machine for the initiator admin messages"""

    TRANSITIONS: Mapping[Tuple[AdminState, AdminEvent], AdminState] = {
        (AdminState.START, AdminEvent.CONNECTED): AdminState.LOGON_REQUESTED,
        (AdminState.LOGON_REQUESTED, AdminEvent.LOGON_SENT): AdminState.LOGON_EXPECTED,
        (AdminState.LOGON_EXPECTED, AdminEvent.LOGON_RECEIVED): AdminState.PENDING,

        # Acceptor heartbeet
        (AdminState.PENDING, AdminEvent.HEARTBEAT_RECEIVED): AdminState.ACKNOWLEDGE_HEARTBEAT,
        (AdminState.ACKNOWLEDGE_HEARTBEAT, AdminEvent.HEARTBEAT_ACKNOWLEDGED): AdminState.PENDING,

        # Test Request
        (AdminState.PENDING, AdminEvent.TEST_REQUEST_RECEIVED): AdminState.TEST_REQUEST_REQUESTED,
        (AdminState.TEST_REQUEST_REQUESTED, AdminEvent.TEST_REQUEST_SENT): AdminState.PENDING,

        # Resend Request
        (AdminState.PENDING, AdminEvent.RESEND_REQUEST_RECEIVED): AdminState.SEQUENCE_RESET_REQUESTED,
        (AdminState.SEQUENCE_RESET_REQUESTED, AdminEvent.SEQUENCE_RESET_SENT): AdminState.PENDING,

        # Sequence Reset
        (AdminState.PENDING, AdminEvent.SEQUENCE_RESET_RECEIVED): AdminState.SET_INCOMING_SEQNUM,
        (AdminState.SET_INCOMING_SEQNUM, AdminEvent.INCOMING_SEQNUM_SET): AdminState.PENDING,

        # Logout
        (AdminState.PENDING, AdminEvent.LOGOUT_RECEIVED): AdminState.ACKNOWLEDGE_LOGOUT,
        (AdminState.ACKNOWLEDGE_LOGOUT, AdminEvent.LOGOUT_ACKNOWLEDGED): AdminState.STOP
    }

    def __init__(self):
        self.state = AdminState.START

    def next_event(self, event: AdminEvent) -> AdminState:
        self.state = self.TRANSITIONS[(self.state, event)]
        return self.state

    def __str__(self) -> str:
        return f"InitiatorStateMachine: state={self.state}"

    __repr__ = __str__


Handler = Callable[[], Awaitable[Optional[AdminEvent]]]


class AsyncAdminStateMachine(AdminStateMachine):

    def __init__(
            self,
            state_handlers: Mapping[Tuple[AdminState, AdminEvent], Handler]
    ) -> None:
        super().__init__()
        self.state_handlers = state_handlers

    async def handle_event(self, event: Optional[AdminEvent]) -> AdminState:
        if event is None:
            return self.state
        if (self.state, event) not in self.state_handlers:
            self.next_event(event)
            return self.state
        else:
            handler = self.state_handlers[(self.state, event)]
            self.next_event(event)
            return await self.handle_event(await handler())
