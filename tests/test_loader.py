from __future__ import annotations

import asyncio
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from dcube_conv.loader import DataCubeLoader
from dcube_conv.model import CubeFile
from dcube_conv.utils import detect_datacube
from tests.test_utils import make_header

HEADER = detect_datacube(make_header())

# Small day folder with loadable cubes, cubes without GPS tags and non-cube stubs.
DATA_DIR = Path("/project/eifel-largen/data-elise/Data-2026-Q2/CZ1/260606-CZ195-451")


async def consume(loader: DataCubeLoader, delay: float = 0.005) -> list[CubeFile]:
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
    def from_file(file: Path) -> CubeFile | None:
        time.sleep(0.001)
        if int(file.stem) % 10 == 0:
            return None
        return CubeFile(path=file, header=HEADER, traces=[], gps_tags=None)

    monkeypatch.setattr(CubeFile, "from_file", staticmethod(from_file))

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
    def from_file(file: Path) -> CubeFile:
        if int(file.stem) == 50:
            raise RuntimeError("boom")
        return CubeFile(path=file, header=HEADER, traces=[], gps_tags=None)

    monkeypatch.setattr(CubeFile, "from_file", staticmethod(from_file))

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
    assert all(cube.header.device == cube.cube_id for cube in cubes)
    assert len(cubes) + stats.n_files_failed == stats.n_files == len(files)
    assert stats.i_files == stats.n_files
    assert stats.processed_percent == pytest.approx(100.0)


async def test_scan_filters_by_header_date(tmp_path: Path) -> None:
    from tests.test_utils import make_header

    files = {}
    for dat_no in range(5):  # 2026-03-09 .. 2026-03-13
        file = tmp_path / f"0309{dat_no:04d}.903"
        file.write_bytes(make_header(dat_no=f"{dat_no:03d}"))
        # Modification times are copy times, unrelated to the recording.
        os.utime(file, (0, datetime(2026, 6, 23, tzinfo=timezone.utc).timestamp()))
        files[dat_no] = file
    (tmp_path / "notes.txt").write_bytes(b"x" * 1024)

    loader = DataCubeLoader(
        directories=[tmp_path],
        min_file_size=0,
        start_date=date(2026, 3, 10),
        end_date=date(2026, 3, 12),
    )
    await loader.prepare()
    assert set(loader) == {files[1], files[2], files[3]}


async def test_scan_keeps_same_file_name_of_different_years(tmp_path: Path) -> None:
    from tests.test_utils import make_header

    files = []
    for year in (25, 26):
        file = tmp_path / f"{year}" / "08050000.903"
        file.parent.mkdir()
        file.write_bytes(make_header(s_date=f"{year}/08/04", dat_no="001"))
        files.append(file)

    loader = DataCubeLoader(directories=[tmp_path], min_file_size=0)
    await loader.prepare()
    assert set(loader) == set(files)


async def test_scan_skips_copies_of_a_recording(tmp_path: Path) -> None:
    from tests.test_utils import make_header

    copies = []
    for folder in ("disk-1", "disk-2"):
        file = tmp_path / folder / "03100000.903"
        file.parent.mkdir()
        file.write_bytes(make_header())
        copies.append(file)

    loader = DataCubeLoader(directories=[tmp_path], min_file_size=0)
    await loader.prepare()
    assert len(set(loader)) == 1

    # A copy of a converted file must not be converted again.
    progress = tmp_path / "run.progress"
    progress.write_text(f"{copies[0]}\n")
    loader = DataCubeLoader(directories=[tmp_path], min_file_size=0)
    loader.set_progress_file(progress)
    await loader.prepare()
    assert set(loader) == set()


def test_cube_file_from_file_reads_header(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def load_datacube(file: Path) -> tuple[list, None]:
        return [object()], None

    monkeypatch.setattr("dcube_conv.model.load_datacube", load_datacube)
    file = tmp_path / "03100000.903"
    file.write_bytes(make_header())

    cube = CubeFile.from_file(file)
    assert cube is not None
    assert cube.header == detect_datacube(make_header())
    assert cube.header.gain == 64
