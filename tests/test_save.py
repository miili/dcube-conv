from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from pyrocko.io import load
from pyrocko.trace import Trace

from dcube_conv import model
from dcube_conv.convert import Converter
from dcube_conv.model import CubeFile
from dcube_conv.utils import detect_datacube
from tests.test_utils import make_header

DAY = 86400.0
HEADER = detect_datacube(make_header())


def make_cube(
    cube_id: str,
    tmin: float,
    station: str = "STA",
    nsamples: int = 1000,
    deltat: float = 0.01,
) -> CubeFile:
    traces = [
        Trace(
            "DC",
            station,
            "",
            channel,
            tmin=tmin,
            deltat=deltat,
            ydata=np.arange(nsamples, dtype=np.int32),
        )
        for channel in ("EPZ", "EPN", "EPE")
    ]
    return CubeFile(
        path=Path(f"00000000.{cube_id}"), header=HEADER, traces=traces, gps_tags=None
    )


def timestamp(*args: int) -> float:
    return datetime(*args, tzinfo=timezone.utc).timestamp()


def template(tmp_path: Path) -> Path:
    return tmp_path / Converter().output_template


def touch_outputs(traces: list[Trace], template: str, additional) -> None:
    for tr in traces:
        file = Path(tr.fill_template(template, **additional))
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()


@pytest.fixture
def concurrency(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Replace the miniSEED writer with a slow stub tracking concurrent calls."""
    state = {"active": 0, "max": 0}
    lock = threading.Lock()

    def slow_save(traces: list[Trace], template: str, additional, **kwargs) -> None:
        with lock:
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
        time.sleep(0.2)
        touch_outputs(traces, template, additional)
        with lock:
            state["active"] -= 1

    monkeypatch.setattr(model, "save", slow_save)
    return state


async def test_same_cube_different_days_write_concurrently(
    tmp_path: Path, concurrency: dict[str, int]
) -> None:
    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube("ABC", t0), make_cube("ABC", t0 + DAY)]

    await asyncio.gather(*(cube.save(template(tmp_path)) for cube in cubes))
    assert concurrency["max"] == 2


async def test_same_day_file_writes_are_serialized(
    tmp_path: Path, concurrency: dict[str, int]
) -> None:
    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube("ABC", t0), make_cube("ABC", t0 + 10.0)]

    await asyncio.gather(*(cube.save(template(tmp_path)) for cube in cubes))
    assert concurrency["max"] == 1


async def test_concurrent_appends_keep_all_samples(tmp_path: Path) -> None:
    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube("ABC", t0 + i * 10.0) for i in range(4)]

    await asyncio.gather(*(cube.save(template(tmp_path)) for cube in cubes))

    files = sorted(tmp_path.rglob("*.D.*"))
    assert len(files) == 3
    for file in files:
        traces = load(str(file))
        assert sum(tr.ydata.size for tr in traces) == 4 * 1000


async def feed(cubes: list[CubeFile], pulled: list[CubeFile]):
    for cube in cubes:
        pulled.append(cube)
        yield cube


async def test_async_save_bounds_cubes_in_flight(
    tmp_path: Path, concurrency: dict[str, int]
) -> None:
    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube(f"C{i:02d}", t0, station=f"S{i}") for i in range(8)]
    converter = Converter(output_path=tmp_path, write_threads=2)

    pulled: list[CubeFile] = []
    task = asyncio.create_task(converter._async_save(feed(cubes, pulled)))
    await asyncio.sleep(0.1)
    # Two cubes are being written, a third waits for a free slot.
    assert len(pulled) == 3

    await task
    assert len(pulled) == 8
    assert concurrency["max"] == 2


async def test_async_save_raises_write_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def failing_save(traces: list[Trace], template: str, additional, **kwargs):
        if traces[0].station == "S1":
            raise OSError("disk full")
        time.sleep(0.05)
        touch_outputs(traces, template, additional)

    monkeypatch.setattr(model, "save", failing_save)

    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube(f"C{i:02d}", t0, station=f"S{i}") for i in range(20)]
    converter = Converter(output_path=tmp_path, write_threads=2)

    pulled: list[CubeFile] = []
    with pytest.raises(ExceptionGroup) as exc_info:
        await converter._async_save(feed(cubes, pulled))
    assert exc_info.group_contains(OSError, match="disk full")
    # No new cubes are started after the failure.
    assert len(pulled) < len(cubes)


async def test_save_returns_bytes_written(tmp_path: Path) -> None:
    t0 = timestamp(2026, 3, 10)
    first = await make_cube("ABC", t0).save(template(tmp_path))
    second = await make_cube("ABC", t0 + 10.0).save(template(tmp_path))

    total = sum(file.stat().st_size for file in tmp_path.rglob("*.D.*"))
    assert first > 0
    assert second > 0
    assert first + second == total


async def test_cube_spanning_new_year_uses_consistent_year(tmp_path: Path) -> None:
    # Dec 31 20:00 to Jan 1 04:00, the mid-time is Jan 1 00:00.
    t0 = timestamp(2025, 12, 31, 20)
    cube = make_cube("ABC", t0, nsamples=2881, deltat=10.0)

    await cube.save(template(tmp_path))

    names = sorted(file.name for file in tmp_path.rglob("*.D.*"))
    assert names == [
        "DC.STA..EPE.D.2026.001",
        "DC.STA..EPN.D.2026.001",
        "DC.STA..EPZ.D.2026.001",
    ]
    assert all(path.parts[-5] == "2026" for path in tmp_path.rglob("*.D.*"))
