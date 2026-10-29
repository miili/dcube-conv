from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from dcube_conv.loader import DataCubeLoader
from dcube_conv.model import CubeTraces

# Small day folder with loadable cubes, cubes without GPS tags and non-cube stubs.
DATA_DIR = Path("/project/eifel-largen/data-elise/Data-2026-Q2/CZ1/260606-CZ195-451")


async def consume(loader: DataCubeLoader, delay: float = 0.005) -> list[CubeTraces]:
    cubes = []
    async for cube in loader.iter_datacubes():
        cubes.append(cube)
        await asyncio.sleep(delay)  # Slow consumer keeps the out queue full
    return cubes


def make_loader(files: set[Path], **kwargs) -> DataCubeLoader:
    loader = DataCubeLoader(**kwargs)
    loader._queue = asyncio.Queue(maxsize=loader.queue_size)
    loader._files = files
    loader._stats.n_files = len(files)
    loader._stats.size_bytes_total = sum(f.stat().st_size for f in files)
    return loader


@pytest.fixture
def fake_files(tmp_path: Path) -> set[Path]:
    files = set()
    for i in range(100):
        file = tmp_path / f"{i:08d}.ABC"
        file.write_bytes(b"x")
        files.add(file)
    return files


async def test_no_cubes_lost_under_backpressure(
    fake_files: set[Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def from_file(file: Path) -> CubeTraces | None:
        time.sleep(0.001)
        if int(file.stem) % 10 == 0:
            return None
        return CubeTraces(path=file, traces=[], gps_tags=None)

    monkeypatch.setattr(CubeTraces, "from_file", staticmethod(from_file))

    loader = make_loader(fake_files, queue_size=2, loading_threads=4)
    cubes = await asyncio.wait_for(consume(loader), timeout=30)

    stats = loader._stats
    assert len(cubes) == 90
    assert stats.n_files_failed == 10
    assert stats.i_files == stats.n_files == 100
    assert stats.processed_percent == pytest.approx(100.0)


async def test_worker_error_propagates(
    fake_files: set[Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def from_file(file: Path) -> CubeTraces:
        if int(file.stem) == 50:
            raise RuntimeError("boom")
        return CubeTraces(path=file, traces=[], gps_tags=None)

    monkeypatch.setattr(CubeTraces, "from_file", staticmethod(from_file))

    loader = make_loader(fake_files, queue_size=2, loading_threads=4)
    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(consume(loader, delay=0), timeout=30)
    assert exc_info.group_contains(RuntimeError, match="boom")


@pytest.mark.skipif(not DATA_DIR.exists(), reason=f"{DATA_DIR} not available")
async def test_load_real_datacubes() -> None:
    loader = DataCubeLoader(
        directories=[DATA_DIR],
        min_file_size=0,
        queue_size=2,
        loading_threads=4,
    )
    await loader.prepare()
    files = set(loader)

    cubes = await asyncio.wait_for(consume(loader), timeout=300)

    stats = loader._stats
    paths = [cube.path for cube in cubes]
    assert cubes
    assert len(set(paths)) == len(paths)
    assert set(paths) <= files
    assert all(cube.traces for cube in cubes)
    assert len(cubes) + stats.n_files_failed == stats.n_files == len(files)
    assert stats.i_files == stats.n_files
    assert stats.processed_percent == pytest.approx(100.0)
