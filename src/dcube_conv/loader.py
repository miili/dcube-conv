from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Iterator

from pydantic import (
    AwareDatetime,
    BaseModel,
    ByteSize,
    DirectoryPath,
    Field,
    PositiveInt,
    PrivateAttr,
    computed_field,
)
from pyrocko.io import datacube

from dcube_conv.model import CubeId, CubeTraces
from dcube_conv.stats import Stats

if TYPE_CHECKING:
    from rich.table import Table

logger = logging.getLogger(__name__)
MB = 1_000_000


class LoadingStats(Stats):
    n_files: int = 0
    i_files: int = 0
    size_bytes_total: int = 0
    bytes_loaded: int = 0
    cube_ids: set[str] = Field(default_factory=set)
    time_started: datetime = Field(default_factory=datetime.now)

    _queue: asyncio.Queue | None = PrivateAttr(default=None)

    def set_queue(self, queue: asyncio.Queue) -> None:
        self._queue = queue

    @computed_field
    @property
    def queue_size(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.qsize()

    @computed_field
    @property
    def queue_maxsize(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.maxsize

    @computed_field
    @property
    def time_elapsed(self) -> timedelta:
        return datetime.now() - self.time_started  # noqa DTZ005

    @computed_field
    @property
    def loading_rate(self) -> float:
        if self.bytes_loaded == 0.0:
            return 0.0
        return self.bytes_loaded / self.time_elapsed.total_seconds()

    @computed_field
    @property
    def time_remaining(self) -> timedelta:
        if self.loading_rate == 0:
            return timedelta(seconds=0)
        return timedelta(
            seconds=(self.size_bytes_total - self.bytes_loaded) / self.loading_rate
        )

    @computed_field
    @property
    def processed_percent(self) -> float:
        return self.bytes_loaded / self.size_bytes_total * 100

    def reset_time(self):
        self.time_started = datetime.now()  # noqa DTZ005

    def _populate_table(self, table: Table) -> None:
        def format_timedelta(td: timedelta) -> str:
            return str(td).split(".")[0]

        def format_bytes(b: int | float) -> str:
            return ByteSize(b).human_readable(decimal=True)

        table.add_row(
            "Progress",
            f"[bold]{self.processed_percent:.1f}%[/bold]",
            f"({self.i_files} / {self.n_files})",
        )
        table.add_row(
            "Processing rate",
            f"{format_bytes(self.loading_rate)}/s",
            f"({format_bytes(self.bytes_loaded)}"
            f" / {format_bytes(self.size_bytes_total)})",
        )
        table.add_row("Remaining time", format_timedelta(self.time_remaining))
        table.add_row("Queue", f"{self.queue_size} / {self.queue_maxsize}")
        table.add_row("Cube IDs", str(len(self.cube_ids)))


class DataCubeLoader(BaseModel):
    directories: list[DirectoryPath] = [DirectoryPath(".")]
    min_file_size: ByteSize = ByteSize(50 * MB)
    queue_size: PositiveInt = 16

    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None

    cube_ids: set[CubeId] = set()

    _stats: LoadingStats = PrivateAttr(default_factory=LoadingStats)
    _files: set[Path] = PrivateAttr(default_factory=set)
    _filenames: set[str] = PrivateAttr(default_factory=set)
    _cube_ids: list[CubeId] = PrivateAttr(default_factory=list)

    _progress_file: Path | None = PrivateAttr(default=None)
    _done_paths: set[Path] = PrivateAttr(default_factory=set)

    _queue: asyncio.Queue[CubeTraces | None] = PrivateAttr(
        default_factory=asyncio.Queue
    )

    async def _scan_datacube_files(self) -> AsyncIterator[Path]:
        logger.info("Adding folders %s", ", ".join(str(d) for d in self.directories))
        for path in self.directories:
            logger.info("Scanning %s", path)
            for file in path.rglob("*.*"):
                if self.cube_ids and file.stem.lstrip(".") not in self.cube_ids:
                    continue
                if file in self._done_paths:
                    continue
                if file.name in self._filenames:
                    logger.warning("Duplicate file %s", file)
                    continue

                stat = file.stat()
                if stat.st_size < self.min_file_size:
                    continue
                if self.start_time and stat.st_mtime < self.start_time.timestamp():
                    continue
                if self.end_time and stat.st_mtime > self.end_time.timestamp():
                    continue
                with file.open("rb") as f:
                    if not datacube.detect(f.read(512)):
                        continue

                self._filenames.add(file.name)
                yield file
                await asyncio.sleep(0)  # Yield to event loop

    async def prepare(self) -> None:
        async for file in self._scan_datacube_files():
            cube_id = file.suffix.lstrip(".").upper()
            self._cube_ids.append(cube_id)
            self._files.add(file)

            self._stats.cube_ids.add(cube_id)
            self._stats.n_files += 1
            self._stats.size_bytes_total += file.stat().st_size

        logger.info(
            "Found %d datacube files, total %s",
            self._stats.n_files,
            ByteSize(self._stats.size_bytes_total).human_readable(decimal=True),
        )

    def set_progress_file(self, file: Path) -> None:
        logger.debug("Setting progress file %s", file)
        self._progress_file = file
        if not file.exists():
            return
        with file.open() as f:
            logger.info("Loading progress from %s", file)
            for line in f:
                self._done_paths.add(Path(line.strip()))

    def __iter__(self) -> Iterator[Path]:
        return iter(self._files)

    async def _fetch_datacubes(self) -> None:
        queue = self._queue
        iter_files = iter(self)
        sem = asyncio.Semaphore(int(round(self.queue_size / 2) + 1))
        done = asyncio.Event()

        self._stats.set_queue(queue)
        self._stats.reset_time()

        async def load() -> None:
            # logger.debug("Fetching %d files", n_files)
            async with sem:
                file = next(iter_files, None)
                if file is None:
                    done.set()
                    return

                cube = await asyncio.to_thread(CubeTraces.from_file, file)
                if cube is None:
                    return
                await queue.put(cube)
                self._stats.bytes_loaded += file.stat().st_size
                self._stats.i_files += 1

        async with asyncio.TaskGroup() as tg:
            while not done.is_set():
                await asyncio.sleep(0)
                async with sem:
                    tg.create_task(load())
        await queue.put(None)

    async def iter_datacubes(self) -> AsyncIterator[CubeTraces]:
        task = asyncio.create_task(self._fetch_datacubes())

        while True:
            cube = await self._queue.get()
            if cube is None:
                break
            yield cube
            self._queue.task_done()

        await task

    def add_done(self, cube: CubeTraces) -> None:
        if not self._progress_file:
            return
        with self._progress_file.open("a") as f:
            f.write(f"{cube.path}\n")
