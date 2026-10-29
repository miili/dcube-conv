from __future__ import annotations

import asyncio
import warnings
from contextlib import suppress
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
from typing import Any, Literal

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

DATETIME_MAX = datetime.max.replace(tzinfo=timezone.utc)


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
    return ByteSize(b).human_readable(decimal=True)


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
            if check_WNRO(utc_gps_offsets, leap_second):
                part.shift(WNRO)
                leap_second = util.utc_gps_offset(t_ref + WNRO)
            part.shift(leap_second)
            traces.append(part)

    return traces, gps_tags
