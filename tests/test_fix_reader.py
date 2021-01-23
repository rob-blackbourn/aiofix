""" Tests for FixReader"""

from typing import Iterator, cast

from jetblack_fixengine.transports.fix_events import (
    FixReadError, FixReadEventType,
    FixReadDataReady
)
from jetblack_fixengine.transports.fix_reader import FixReader


def _bytes_writer(buf: bytes, chunk_size: int = -1) -> Iterator[bytes]:
    if chunk_size == -1:
        yield buf
    else:
        start, end = 0, chunk_size
        while start < len(buf):
            yield buf[start:end]
            start, end = end, end + chunk_size


def test_read_valid_buffer():
    """Test for read"""

    reader = FixReader(
        sep=b'|',
        convert_sep_to_soh_for_checksum=True,
        validate=True
    )
    messages = [
        b'8=FIX.4.4|9=94|35=3|49=A|56=AB|128=B1|34=214|50=U1|52=20100304-09:42:23.130|45=176|371=15|372=X|373=1|58=txt|10=058|',
        b'8=FIX.4.4|9=117|35=AD|49=A|56=B|34=2|50=1|57=M|52=20100219-14:33:32.258|568=1|569=0|263=1|580=1|75=20100218|60=20100218-00:00:00.000|10=202|',
        b'8=FIX.4.4|9=122|35=D|49=CLIENT12|56=B|34=215|52=20100225-19:41:57.316|11=13346|1=Marcel|21=1|54=1|60=20100225-19:39:52.020|40=2|44=5|59=0|10=072|'
    ]
    input_buf = b''
    for message in messages:
        input_buf += message

    writer = _bytes_writer(input_buf, 200)
    # buf = next(writer)
    # reader.receive(buf)
    done = False
    message_index = 0
    while not done:
        fix_event = reader.next_event()
        if fix_event.event_type == FixReadEventType.EOF:
            done = True
        elif fix_event.event_type == FixReadEventType.NEEDS_MORE_DATA:
            try:
                buf = next(writer)
                reader.receive(buf)
            except StopIteration:
                reader.receive(b'')
        elif fix_event.event_type == FixReadEventType.DATA_READY:
            fix_message = cast(FixReadDataReady, fix_event)
            assert fix_message.data == messages[message_index]
            message_index += 1
        else:
            assert False


def test_read_corrupt_buffer():
    """Test for read"""

    reader = FixReader(
        sep=b'|',
        convert_sep_to_soh_for_checksum=True,
        validate=True
    )

    input_buf = b'8=FIX.4.4|9=94|35=3|49=A|56=AB|128=B1|34=214|50=U1|52=201003'

    writer = _bytes_writer(input_buf, 200)
    buf = next(writer)
    reader.receive(buf)
    done = False
    try:
        while not done:
            fix_event = reader.next_event()
            if fix_event.event_type == FixReadEventType.EOF:
                done = True
            elif fix_event.event_type == FixReadEventType.NEEDS_MORE_DATA:
                try:
                    buf = next(writer)
                    reader.receive(buf)
                except StopIteration:
                    reader.receive(b'')
            elif fix_event.event_type == FixReadEventType.DATA_READY:
                continue
            else:
                assert False
    except FixReadError:
        assert True
    except:  # pylint: disable=bare-except
        assert False
    else:
        assert False
