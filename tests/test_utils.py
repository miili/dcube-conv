from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pyrocko.io import datacube
from pyrocko.io.datacube import DataCubeError

from dcube_conv.utils import DATACUBE_HEADER_SIZE, detect_datacube


def make_header(
    s_date: str = "26/03/09",
    s_time: str = "12:43:38",
    dat_no: str = "001",
    terminator: bytes = b"\x80",
) -> bytes:
    header = (
        b"\xf0GIPP_V=V3.3_10A;DEV_NO=903;E_NAME=EGER-Large-N;P_AMPL=64;CH_NUM=3;"
        b"S_RATE=200;D_FILT=31;GPS_PO=>SIP+50+012+0000;"
        + f"S_TIME={s_time};S_DATE={s_date};D_VOLT=3035;DAT_NO={dat_no}".encode()
        + terminator
        + b">XPV015.1+5008140+01236586+0042312"
    )
    return header.ljust(DATACUBE_HEADER_SIZE, b"\x00")


def test_detect_datacube() -> None:
    data = make_header()
    assert datacube.detect(data)

    header = detect_datacube(data)
    assert header.device == "903"
    assert header.n_channels == 3
    assert header.sampling_rate == 200
    assert header.gain == 64
    assert header.file_number == 1
    assert header.session_start == datetime(2026, 3, 9, 12, 43, 38, tzinfo=timezone.utc)
    assert header.date == date(2026, 3, 10)
    assert header.start_time == datetime(2026, 3, 10, tzinfo=timezone.utc)


def test_detect_datacube_first_file_starts_with_session() -> None:
    header = detect_datacube(make_header(dat_no="000"))
    assert header.date == date(2026, 3, 9)
    assert header.start_time == header.session_start


def test_detect_datacube_gps_block_terminator() -> None:
    header = detect_datacube(make_header(dat_no="046", terminator=b"\xa0"))
    assert header.file_number == 46


def test_detect_datacube_gps_week_rollover() -> None:
    # Cube 832 reports dates 1024 weeks too early.
    header = detect_datacube(make_header(s_date="06/11/14", s_time="15:56:23"))
    assert header.session_start == datetime(
        2026, 6, 30, 15, 56, 23, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"x" * DATACUBE_HEADER_SIZE,
        make_header()[:100],
        make_header(terminator=b"").replace(b"\x80", b"\x00"),
        make_header(s_date="26/13/09"),
        make_header().replace(b"DAT_NO", b"XXX_NO"),
    ],
)
def test_detect_datacube_raises(data: bytes) -> None:
    with pytest.raises(DataCubeError):
        detect_datacube(data)
