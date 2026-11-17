from __future__ import annotations

import asyncio
import re
import warnings
from contextlib import suppress
from datetime import date, datetime, time, timedelta, timezone
from functools import cache
from pathlib import Path
from typing import Any, Literal, NamedTuple

import aiohttp
import numpy as np
from pydantic import ByteSize
from pyrocko import datacube_ext, signal_ext, util
from pyrocko.io.datacube import (
    WNRO,
    DataCubeError,
    GPSTags,
    analyse_gps_tags,
    check_WNRO,
    get_extended_timing_context,
)
from pyrocko.trace import NoData, Trace


def datetime_now() -> datetime:
    return datetime.now(tz=timezone.utc)


ElevationModel = Literal[
    "aster30m",
    "etopo1",
    "eudem25m",
    "mapzen",
    "ned10m",
    "nzdem8m",
    "srtm30m",
    "emod2018",
    "gebco2020",
    "bkg200m",
    "swisstopo-2m",
]

LAST_REQUEST = datetime_now()


async def get_elevation(
    lat: float,
    lon: float,
    model: ElevationModel = "aster30m",
    interpolation: Literal["nearest", "bilinear", "cubic"] = "bilinear",
) -> float:
    """Get elevation from OpenTopoData API.

    Args:
        lat (float): Latitude in [deg].
        lon (float): Longitude in [deg].
        model (ElevationModel, optional): The elevation model to use.
            Defaults to "aster30m".
        interpolation (Literal["nearest", "bilinear", "cubic"], optional):
            The interpolation method. Defaults to "bilinear".

    Returns:
        float: The elevation in [m].
    """
    global LAST_REQUEST
    if (datetime_now() - LAST_REQUEST).total_seconds() < 1.0:
        await asyncio.sleep(1.0)

    async with (
        aiohttp.ClientSession() as session,
        session.get(
            f"https://api.opentopodata.org/v1/{model}",
            params={
                "locations": f"{lat},{lon}",
                "interpolation": interpolation,
            },
        ) as response,
    ):
        LAST_REQUEST = datetime_now()
        data = await response.json()
        return data["results"][0]["elevation"]


def format_bytes(b: int | float) -> str:
    """Decimal data size with a space before the unit, e.g. 23.9 TB."""
    return re.sub(r"(?<=\d)(?=[A-Za-z])", " ", ByteSize(b).human_readable(decimal=True))


@cache
def _leap_second_times() -> np.ndarray:
    return np.array(
        [t + util.gps_utc_offset(t) for t, _ in util.read_leap_seconds2()],
        dtype=float,
    )


def load_datacube(file: Path) -> tuple[list[Trace], Any]:
    """Load GPS time-corrected traces and raw GPS tags from a DATA-CUBE³ file.

    Lean version of `pyrocko.io.datacube.iload`. It reuses the GPS tags from
    the data load instead of reading the file a second time, and only scans
    neighbouring files of the directory if the file has no GPS fix.

    Args:
        file (Path): The DATA-CUBE³ file.

    Returns:
        tuple[list[Trace], Any]: The traces and the raw GPS tags tuple.
    """
    with file.open("rb") as f:
        try:
            header, data_arrays, gps_tags, nsamples, _ = datacube_ext.load(
                f.fileno(), 2, 0, -1, None
            )
        except datacube_ext.DataCubeError as exc:
            error = DataCubeError(str(exc))
            error.set_context("filename", str(file))
            raise error from exc

    header = dict(header)
    deltat = 1.0 / int(header["S_RATE"])
    nchannels = int(header["CH_NUM"])

    tags = GPSTags.from_tuple(gps_tags)
    if np.sum(tags.fix) == 0:
        # No GPS fix in this file, borrow timing from neighbouring files.
        (
            ipos,
            times,
            fix,
            sat_count,
            utc_gps_offsets,
            timing_header,
            offset,
            timing_nsamples,
        ) = get_extended_timing_context(str(file))
    else:
        ipos, times, fix, sat_count = tags[:4]
        utc_gps_offsets = tags.gps_utc_offsets
        timing_header, offset, timing_nsamples = header, 0, nsamples

    tmin, tmax, icontrol, tcontrol, _ = analyse_gps_tags(
        timing_header, (ipos, times, fix, sat_count), offset, timing_nsamples
    )

    if icontrol is None:
        warnings.warn(
            "No usable GPS timestamps found. Using datacube header "
            f'information to guess time. (file: "{file}")',
            stacklevel=2,
        )

    tmin_ip = round(tmin / deltat) * deltat
    tmax_ip = round(tmax / deltat) * deltat
    nsamples_ip = round((tmax_ip - tmin_ip) / deltat) + 1
    # to prevent problems with rounding errors:
    tmax_ip = tmin_ip + (nsamples_ip - 1) * deltat

    leaps = _leap_second_times()

    if icontrol is not None:
        ncontrol_this = np.sum(np.logical_and(icontrol >= 0, icontrol < nsamples))
        if ncontrol_this <= 1:
            warnings.warn(
                "Extrapolating GPS time information from directory context "
                f'(insufficient number of GPS timestamps in file: "{file}").',
                stacklevel=2,
            )

    traces = []
    for ichannel in range(nchannels):
        arr = data_arrays[ichannel]
        if arr.size != nsamples:
            raise DataCubeError(f"Unexpected number of samples in {file}")

        if icontrol is not None:
            ydata = np.empty(nsamples_ip, dtype=float)
            try:
                signal_ext.antidrift(
                    icontrol, tcontrol, arr.astype(float), tmin_ip, deltat, ydata
                )
            except signal_ext.Error as exc:
                error = DataCubeError(str(exc))
                error.set_context("filename", str(file))
                raise error from exc
            ydata = np.round(ydata).astype(arr.dtype)
        else:
            ydata = arr

        tr = Trace(
            "",
            header["DEV_NO"],
            "",
            f"p{ichannel}",
            deltat=deltat,
            ydata=ydata,
            tmin=tmin_ip,
            meta=header,
        )

        bleaps = np.logical_and(tmin_ip <= leaps, leaps < tmax_ip)
        if np.any(bleaps):
            if np.sum(bleaps) != 1:
                raise DataCubeError(f"Multiple leap seconds within {file}")
            tcut = leaps[bleaps][0]
            parts = []
            for tmin_cut, tmax_cut in ((tr.tmin, tcut), (tcut, tr.tmax + tr.deltat)):
                with suppress(NoData):
                    parts.append(tr.chop(tmin_cut, tmax_cut, inplace=False))
        else:
            parts = [tr]

        for part in parts:
            t_ref = 0.5 * (part.tmin + part.tmax)
            leap_second = util.utc_gps_offset(t_ref)
            # A single bogus UTC offset in the GPS tags fails the rollover
            # check. A rolled over receiver reports times before the last
            # rollover, never recent ones.
            if (
                check_WNRO(utc_gps_offsets, leap_second)
                and t_ref < GPS_LAST_WNRO.timestamp()
            ):
                part.shift(WNRO)
                leap_second = util.utc_gps_offset(t_ref + WNRO)
            part.shift(leap_second)
            traces.append(part)

    return traces, gps_tags


DATACUBE_HEADER_SIZE = 512
DATACUBE_BLOCK_MARKER = re.compile(rb"[\x80-\xef\xf1-\xff]")

# GPS receivers with outdated firmware report dates from before the last GPS
# week number rollover, 1024 weeks too early.
GPS_LAST_WNRO = datetime(2019, 4, 7, tzinfo=timezone.utc)
GPS_WNRO_PERIOD = timedelta(weeks=1024)

# GPS tags at the end of a file used to time its last sample.
N_END_TAGS = 64


def gps_end_time(tags: GPSTags, nsamples: int, sampling_rate: float) -> datetime | None:
    """Time of the last sample of a file from its last GPS tags, UTC.

    The cube clock drifts by up to about 30 s per day. After a few months, the
    nominal end, from the file start and the sample count, is up to an hour
    late. The GPS tags give the true time of the samples they tag.

    Args:
        tags (GPSTags): The GPS tags of the file.
        nsamples (int): Number of samples in the file.
        sampling_rate (float): Nominal sampling rate in [Hz].

    Returns:
        datetime | None: The end time, None without a GPS fix.
    """
    fix = tags.fix > 0
    if not fix.any():
        return None
    deltat = 1.0 / sampling_rate
    positions = tags.position[fix][-N_END_TAGS:]
    times = tags.gps_time[fix][-N_END_TAGS:]
    gps_end = float(np.median(times - positions * deltat)) + (nsamples - 1) * deltat
    if gps_end < GPS_LAST_WNRO.timestamp():
        gps_end += GPS_WNRO_PERIOD.total_seconds()
    return datetime.fromtimestamp(
        gps_end + util.utc_gps_offset(gps_end), tz=timezone.utc
    )


SessionId = tuple[str, datetime]


class DataCubeHeader(NamedTuple):
    """Minimal DATA-CUBE³ file header."""

    device: str
    n_channels: int
    sampling_rate: int
    gain: int
    """Preamplifier gain (P_AMPL)."""
    file_number: int
    session_start: datetime
    """Start of the recording session, UTC."""

    @property
    def session_id(self) -> SessionId:
        """Identifies the recording session, the cube does not move within it."""
        return self.device, self.session_start

    @property
    def recording_id(self) -> tuple[str, datetime, int]:
        """Identifies the recorded file across copies. File names repeat yearly."""
        return self.device, self.session_start, self.file_number

    @property
    def date(self) -> date:
        """Day the file covers. Files after the first start at midnight."""
        return self.session_start.date() + timedelta(days=self.file_number)

    @property
    def start_time(self) -> datetime:
        """Start time of the file, UTC within a few seconds."""
        if self.file_number == 0:
            return self.session_start
        return datetime.combine(self.date, time(), tzinfo=timezone.utc)


def detect_datacube(data: bytes) -> DataCubeHeader:
    """Parse the header of a DATA-CUBE³ file.

    Args:
        data (bytes): The first `DATACUBE_HEADER_SIZE` bytes of the file.

    Returns:
        DataCubeHeader: The parsed header.

    Raises:
        DataCubeError: If the data is not a DATA-CUBE³ header.
    """
    if len(data) < DATACUBE_HEADER_SIZE or data[0] >> 4 != 0xF:
        raise DataCubeError("Not a DATA-CUBE³ file")

    # The header ends with the first data (0x80) or GPS (0xa0) block marker.
    end = DATACUBE_BLOCK_MARKER.search(data, 1)
    if end is None:
        raise DataCubeError("DATA-CUBE³ header is not terminated")

    entries = data[1 : end.start()].replace(b"\xf0", b"").split(b";")
    fields = dict(entry.split(b"=", 1) for entry in entries if b"=" in entry)
    try:
        session_start = datetime.strptime(
            (fields[b"S_DATE"] + fields[b"S_TIME"]).decode(),
            "%y/%m/%d%H:%M:%S",
        ).replace(tzinfo=timezone.utc)
        if session_start < GPS_LAST_WNRO:
            session_start += GPS_WNRO_PERIOD
        return DataCubeHeader(
            device=fields[b"DEV_NO"].decode(),
            n_channels=int(fields[b"CH_NUM"]),
            sampling_rate=int(fields[b"S_RATE"]),
            gain=int(fields[b"P_AMPL"]),
            file_number=int(fields[b"DAT_NO"]),
            session_start=session_start,
        )
    except (KeyError, ValueError, UnicodeDecodeError) as exc:
        raise DataCubeError(f"Invalid DATA-CUBE³ header: {exc!r}") from exc
