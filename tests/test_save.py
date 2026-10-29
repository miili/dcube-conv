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
from dcube_conv.model import CubeTraces

DAY = 86400.0


def make_cube(
    cube_id: str,
    tmin: float,
    station: str = "STA",
    nsamples: int = 1000,
    deltat: float = 0.01,
) -> CubeTraces:
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
    return CubeTraces(path=Path(f"00000000.{cube_id}"), traces=traces, gps_tags=None)


def timestamp(*args: int) -> float:
    return datetime(*args, tzinfo=timezone.utc).timestamp()


def template(tmp_path: Path) -> Path:
    return tmp_path / Converter().output_template


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
        for tr in traces:
            file = Path(tr.fill_template(template, **additional))
            file.parent.mkdir(parents=True, exist_ok=True)
            file.touch()
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


async def feed(cubes: list[CubeTraces], pulled: list[CubeTraces]):
    for cube in cubes:
        pulled.append(cube)
        yield cube


async def test_async_save_bounds_cubes_in_flight(
    tmp_path: Path, concurrency: dict[str, int]
) -> None:
    t0 = timestamp(2026, 3, 10)
    cubes = [make_cube(f"C{i:02d}", t0, station=f"S{i}") for i in range(8)]
    converter = Converter(output_path=tmp_path, write_threads=2)

    pulled: list[CubeTraces] = []
    task = asyncio.create_task(converter._async_save(feed(cubes, pulled)))
    await asyncio.sleep(0.1)
    # Two cubes are being written, a third waits for a free slot.
    assert len(pulled) == 3

    await task
    assert len(pulled) == 8
    assert concurrency["max"] == 2
