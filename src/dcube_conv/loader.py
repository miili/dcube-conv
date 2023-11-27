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
    size_bytes_total: int = 0
    bytes_loaded: int = 0
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
            self.reset_time()
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
        table.add_row(
            "Progress",
            f"[bold]{self.processed_percent:.1f}%[/bold]"
            f" ({ByteSize(self.bytes_loaded).human_readable(decimal=True)}"
            f"/{ByteSize(self.size_bytes_total).human_readable(decimal=True)})",
        )
        table.add_row(
            "Loading rate",
            f"{ByteSize(self.loading_rate).human_readable()}/s",
        )
        table.add_row("Remaining time", str(self.time_remaining))
        table.add_row("Queue", f"{self.queue_size}/{self.queue_maxsize}")


class DataCubeLoader(BaseModel):
    directories: list[DirectoryPath] = [DirectoryPath(".")]
    min_file_size: ByteSize = ByteSize(15 * MB)
    queue_size: PositiveInt = 20

    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None

    cube_ids: set[CubeId] = set()

    _stats: LoadingStats = PrivateAttr(default_factory=LoadingStats)
    _files: list[Path] = PrivateAttr(default_factory=list)
    _cube_ids: list[CubeId] = PrivateAttr(default_factory=list)

    _progress_file: Path | None = PrivateAttr(default=None)
    _done_paths: set[Path] = PrivateAttr(default_factory=set)

    async def _scan_datacube_files(self) -> AsyncIterator[Path]:
        logger.info("Adding folders %s", ", ".join(str(d) for d in self.directories))
        for path in self.directories:
            logger.info("Scanning %s", path)
            for file in path.rglob("*.*"):
                if self.cube_ids and file.stem.lstrip(".") not in self.cube_ids:
                    continue
                if file in self._done_paths:
                    continue

                file_stats = file.stat()
                if file_stats.st_size < self.min_file_size:
                    continue
                if (
                    self.start_time
                    and file_stats.st_mtime < self.start_time.timestamp()
                ):
                    continue
                if self.end_time and file_stats.st_mtime > self.end_time.timestamp():
                    continue

                with file.open("rb") as f:
                    if not datacube.detect(f.read(512)):
                        continue
                yield file
                await asyncio.sleep(0)

    async def prepare(self) -> None:
        async for file in self._scan_datacube_files():
            self._stats.n_files += 1
            self._stats.size_bytes_total += file.stat().st_size
            self._files.append(file)
            self._cube_ids.append(file.stem.lstrip("."))
        logger.info(
            "Found %d datacube files (%s/s)",
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

    async def iter_datacubes(self) -> AsyncIterator[CubeTraces]:
        queue = asyncio.Queue(maxsize=self.queue_size)
        self._stats.set_queue(queue)
        iterator = iter(self)

        async def worker(file: Path) -> None:
            await queue.put(await asyncio.to_thread(CubeTraces.from_file, file))
            self._stats.bytes_loaded += file.stat().st_size

        async def fetch(task_group: asyncio.TaskGroup, n_files: int = 1) -> None:
            # logger.debug("Fetching %d files", n_files)
            for _ in range(n_files):
                file = next(iterator, None)
                if file is None:
                    await queue.put(None)
                    break
                task_group.create_task(worker(file))

        async with asyncio.TaskGroup() as tg:
            tg.create_task(fetch(tg, queue.maxsize))

            while True:
                cube = await queue.get()
                if cube is None:
                    break
                yield cube
                queue.task_done()
                tg.create_task(fetch(tg, n_files=1))

    def add_done(self, cube: CubeTraces) -> None:
        if not self._progress_file:
            return
        with self._progress_file.open("a") as f:
            f.write(f"{cube.path}\n")
