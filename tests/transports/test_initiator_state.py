"""Tests for initiator state"""

from typing import Optional

import pytest

from jetblack_fixengine.transports.initiator_state import (
    AdminState,
    AdminEvent,
    AdminStateMachine,
    AsyncAdminStateMachine
)


def test_admin_states():
    """Test admin state"""
    state_machine = AdminStateMachine()
    assert state_machine.state == AdminState.START

    # Logon
    next_state = state_machine.next_event(AdminEvent.CONNECTED)
    assert next_state == AdminState.LOGON_REQUESTED
    next_state = state_machine.next_event(AdminEvent.LOGON_SENT)
    assert next_state == AdminState.LOGON_EXPECTED
    next_state = state_machine.next_event(AdminEvent.LOGON_RECEIVED)
    assert next_state == AdminState.PENDING

    # Heartbeat
    next_state = state_machine.next_event(AdminEvent.HEARTBEAT_RECEIVED)
    assert next_state == AdminState.ACKNOWLEDGE_HEARTBEAT
    next_state = state_machine.next_event(AdminEvent.HEARTBEAT_ACKNOWLEDGED)
    assert next_state == AdminState.PENDING

    # Test request
    next_state = state_machine.next_event(AdminEvent.TEST_REQUEST_RECEIVED)
    assert next_state == AdminState.TEST_REQUEST_REQUESTED
    next_state = state_machine.next_event(AdminEvent.TEST_REQUEST_SENT)
    assert next_state == AdminState.PENDING

    # Resend request
    next_state = state_machine.next_event(AdminEvent.RESEND_REQUEST_RECEIVED)
    assert next_state == AdminState.SEQUENCE_RESET_REQUESTED
    next_state = state_machine.next_event(AdminEvent.SEQUENCE_RESET_SENT)
    assert next_state == AdminState.PENDING

    # Sequence Reset
    next_state = state_machine.next_event(AdminEvent.SEQUENCE_RESET_RECEIVED)
    assert next_state == AdminState.SET_INCOMING_SEQNUM
    next_state = state_machine.next_event(AdminEvent.INCOMING_SEQNUM_SET)
    assert next_state == AdminState.PENDING

    # Logout
    next_state = state_machine.next_event(AdminEvent.LOGOUT_RECEIVED)
    assert next_state == AdminState.ACKNOWLEDGE_LOGOUT
    next_state = state_machine.next_event(AdminEvent.LOGOUT_ACKNOWLEDGED)
    assert next_state == AdminState.STOP


@pytest.mark.asyncio
async def test_async_admin_state_machine():
    async def send_logon() -> Optional[AdminEvent]:
        return AdminEvent.LOGON_SENT

    async def logon_received() -> Optional[AdminEvent]:
        return None

    async def acknowledge_heartbeat() -> Optional[AdminEvent]:
        return AdminEvent.HEARTBEAT_ACKNOWLEDGED

    async def send_test_request() -> Optional[AdminEvent]:
        return AdminEvent.TEST_REQUEST_SENT

    async def send_sequence_reset() -> Optional[AdminEvent]:
        return AdminEvent.SEQUENCE_RESET_SENT

    async def acknowledge_logout() -> Optional[AdminEvent]:
        return AdminEvent.LOGOUT_ACKNOWLEDGED

    state_machine = AsyncAdminStateMachine({
        (AdminState.START, AdminEvent.CONNECTED): send_logon,
        (AdminState.LOGON_EXPECTED, AdminEvent.LOGON_RECEIVED): logon_received,

        (AdminState.PENDING, AdminEvent.HEARTBEAT_RECEIVED): acknowledge_heartbeat,
        (AdminState.PENDING, AdminEvent.TEST_REQUEST_RECEIVED): send_test_request,
        (AdminState.PENDING, AdminEvent.RESEND_REQUEST_RECEIVED): send_sequence_reset,
        (AdminState.PENDING, AdminEvent.LOGOUT_RECEIVED): acknowledge_logout
    })

    assert state_machine.state == AdminState.START
    await state_machine.handle_event(AdminEvent.CONNECTED)
    assert state_machine.state == AdminState.LOGON_EXPECTED
    await state_machine.handle_event(AdminEvent.LOGON_RECEIVED)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEvent.HEARTBEAT_RECEIVED)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEvent.TEST_REQUEST_RECEIVED)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEvent.RESEND_REQUEST_RECEIVED)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEvent.LOGOUT_RECEIVED)
    assert state_machine.state == AdminState.STOP
