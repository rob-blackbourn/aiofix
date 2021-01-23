"""Tests for initiator state"""

from typing import Optional

import pytest

from jetblack_fixparser.fix_message import FixMessage

from jetblack_fixengine.transports.initiator_state import (
    AdminState,
    AdminEventType,
    AdminStateMachine,
    AsyncAdminStateMachine
)


def test_admin_states():
    """Test admin state"""
    state_machine = AdminStateMachine()
    assert state_machine.state == AdminState.START

    # Logon
    next_state = state_machine.next_event(AdminEventType.CONNECTED)
    assert next_state == AdminState.LOGON_REQUESTED
    next_state = state_machine.next_event(AdminEventType.LOGON_SENT)
    assert next_state == AdminState.LOGON_EXPECTED
    next_state = state_machine.next_event(AdminEventType.LOGON_RECEIVED)
    assert next_state == AdminState.LOGON_ACCEPTED
    next_state = state_machine.next_event(AdminEventType.LOGON_ACK)
    assert next_state == AdminState.PENDING

    # Heartbeat
    next_state = state_machine.next_event(AdminEventType.HEARTBEAT_RECEIVED)
    assert next_state == AdminState.ACKNOWLEDGE_HEARTBEAT
    next_state = state_machine.next_event(
        AdminEventType.HEARTBEAT_ACK)
    assert next_state == AdminState.PENDING

    # Test request
    next_state = state_machine.next_event(AdminEventType.TEST_REQUEST_RECEIVED)
    assert next_state == AdminState.TEST_REQUEST_REQUESTED
    next_state = state_machine.next_event(AdminEventType.TEST_REQUEST_SENT)
    assert next_state == AdminState.PENDING

    # Resend request
    next_state = state_machine.next_event(
        AdminEventType.RESEND_REQUEST_RECEIVED)
    assert next_state == AdminState.SEQUENCE_RESET_REQUESTED
    next_state = state_machine.next_event(AdminEventType.SEQUENCE_RESET_SENT)
    assert next_state == AdminState.PENDING

    # Sequence Reset
    next_state = state_machine.next_event(
        AdminEventType.SEQUENCE_RESET_RECEIVED)
    assert next_state == AdminState.SET_INCOMING_SEQNUM
    next_state = state_machine.next_event(AdminEventType.INCOMING_SEQNUM_SET)
    assert next_state == AdminState.PENDING

    # Logout
    next_state = state_machine.next_event(AdminEventType.LOGOUT_RECEIVED)
    assert next_state == AdminState.ACKNOWLEDGE_LOGOUT
    next_state = state_machine.next_event(AdminEventType.LOGOUT_ACK)
    assert next_state == AdminState.STOP


@pytest.mark.asyncio
async def test_async_admin_state_machine():
    async def send_logon(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.LOGON_SENT

    async def logon_received(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.LOGON_ACK

    async def acknowledge_logon(_fix_message: Optional[FixMessage]) -> Optional[AdminEventType]:
        return None

    async def acknowledge_heartbeat(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.HEARTBEAT_ACK

    async def send_test_request(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.TEST_REQUEST_SENT

    async def send_sequence_reset(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.SEQUENCE_RESET_SENT

    async def acknowledge_logout(_fix_mesage: Optional[FixMessage]) -> Optional[AdminEventType]:
        return AdminEventType.LOGOUT_ACK

    state_machine = AsyncAdminStateMachine({
        (AdminState.START, AdminEventType.CONNECTED): send_logon,
        (AdminState.LOGON_EXPECTED, AdminEventType.LOGON_RECEIVED): logon_received,
        (AdminState.LOGON_ACCEPTED, AdminEventType.LOGON_ACK): acknowledge_logon,

        (AdminState.PENDING, AdminEventType.HEARTBEAT_RECEIVED): acknowledge_heartbeat,
        (AdminState.PENDING, AdminEventType.TEST_REQUEST_RECEIVED): send_test_request,
        (AdminState.PENDING, AdminEventType.RESEND_REQUEST_RECEIVED): send_sequence_reset,
        (AdminState.PENDING, AdminEventType.LOGOUT_RECEIVED): acknowledge_logout
    })

    assert state_machine.state == AdminState.START
    await state_machine.handle_event(AdminEventType.CONNECTED, None)
    assert state_machine.state == AdminState.LOGON_EXPECTED
    await state_machine.handle_event(AdminEventType.LOGON_RECEIVED, None)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEventType.HEARTBEAT_RECEIVED, None)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEventType.TEST_REQUEST_RECEIVED, None)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEventType.RESEND_REQUEST_RECEIVED, None)
    assert state_machine.state == AdminState.PENDING

    await state_machine.handle_event(AdminEventType.LOGOUT_RECEIVED, None)
    assert state_machine.state == AdminState.STOP
