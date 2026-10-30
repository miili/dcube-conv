from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Iterator

from pydantic import (
    BaseModel,
    ByteSize,
    DirectoryPath,
    Field,
    PositiveInt,
    PrivateAttr,
    computed_field,
)
from pyrocko.io.datacube import DataCubeError

from dcube_conv.model import CubeFile, CubeId
from dcube_conv.stats import Stats
from dcube_conv.utils import (
    DATACUBE_HEADER_SIZE,
    DataCubeHeader,
    detect_datacube,
    format_bytes,
)

if TYPE_CHECKING:
    from rich.table import Table

logger = logging.getLogger(__name__)
MB = 1_000_000


class LoadingStats(Stats):
    n_files: int = 0
    i_files: int = 0
    n_files_failed: int = 0
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

    def add_bytes(self, nbytes: int) -> None:
        if self.bytes_loaded == 0:
            self.time_started = datetime.now()  # noqa DTZ005
        self.bytes_loaded += nbytes

    def _populate_table(self, table: Table) -> None:
        def format_timedelta(td: timedelta) -> str:
            return str(td).split(".")[0]

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
        table.add_row("Queue", f"{self.queue_size}/{self.queue_maxsize}")
        table.add_row("Failed files", str(self.n_files_failed))
        table.add_row("Cube IDs", str(len(self.cube_ids)))


class DataCubeLoader(BaseModel):
    directories: list[DirectoryPath] = [DirectoryPath(".")]
    min_file_size: ByteSize = ByteSize(60 * MB)
    queue_size: PositiveInt = 16
    loading_threads: PositiveInt = 16

    start_date: date | None = None
    end_date: date | None = None

    cube_selection: set[CubeId] = set()

    _stats: LoadingStats = PrivateAttr(default_factory=LoadingStats)
    _files: set[Path] = PrivateAttr(default_factory=set)
    _cube_ids: list[CubeId] = PrivateAttr(default_factory=list)

    _progress_file: Path | None = PrivateAttr(default=None)
    _done_paths: set[Path] = PrivateAttr(default_factory=set)

    _queue: asyncio.Queue[CubeFile | None] = PrivateAttr(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )

    async def _scan_datacube_files(
        self,
    ) -> AsyncIterator[tuple[Path, DataCubeHeader]]:
        logger.info("Adding folders %s", ", ".join(str(d) for d in self.directories))
        for path in self.directories:
            logger.info("Scanning %s", path)

            for ifile, file in enumerate(path.rglob("*.*")):
                if file.is_dir():
                    continue
                if (
                    self.cube_selection
                    and file.suffix.lstrip(".").upper() not in self.cube_selection
                ):
                    continue
                if file.stat().st_size < self.min_file_size:
                    continue
                with file.open("rb") as f:
                    try:
                        header = detect_datacube(f.read(DATACUBE_HEADER_SIZE))
                    except DataCubeError:
                        continue
                # File modification times are unreliable, they change on copy.
                if self.start_date and header.date < self.start_date:
                    continue
                if self.end_date and header.date > self.end_date:
                    continue

                yield file, header
                if ifile % 25 == 0:
                    await asyncio.sleep(0)  # Yield to event loop

    async def prepare(self) -> None:
        self._queue = asyncio.Queue(maxsize=self.queue_size)

        recordings: defaultdict[tuple, list[Path]] = defaultdict(list)
        async for file, header in self._scan_datacube_files():
            recordings[header.recording_id].append(file)

        for copies in recordings.values():
            if len(copies) > 1:
                logger.warning(
                    "Duplicate recording in %s", ", ".join(str(f) for f in copies)
                )
            # Also skips copies of converted files, they would be appended twice.
            if self._done_paths.intersection(copies):
                continue
            file = max(copies, key=lambda f: f.stat().st_size)

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
        logger.debug("Fetching datacube files")
        out_queue = self._queue
        stats = self._stats
        stats.set_queue(out_queue)

        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.loading_threads)

        files_queue = asyncio.Queue()
        for file in self._files:
            files_queue.put_nowait(file)

        async def load() -> None:
            while True:
                file = await files_queue.get()
                try:
                    cube = await loop.run_in_executor(
                        executor,
                        CubeFile.from_file,
                        file,
                    )
                    if cube is None:
                        stats.n_files_failed += 1
                    else:
                        await out_queue.put(cube)
                    stats.add_bytes(file.stat().st_size)
                    stats.i_files += 1
                finally:
                    # Only after the cube is handed over, else join() returns
                    # while workers still wait on out_queue and get cancelled.
                    files_queue.task_done()

        try:
            async with asyncio.TaskGroup() as tg:
                workers = [
                    tg.create_task(load()) for _ in range(self.loading_threads * 2)
                ]
                await files_queue.join()
                for worker in workers:
                    worker.cancel()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            await out_queue.put(None)

    async def iter_datacubes(self) -> AsyncIterator[CubeFile]:
        logger.info("Iterating datacube files")
        task = asyncio.create_task(self._fetch_datacubes())

        while True:
            cube = await self._queue.get()
            if cube is None:
                break
            yield cube
            self._queue.task_done()

        await task

    def add_done(self, cube: CubeFile) -> None:
        if not self._progress_file:
            return
        with self._progress_file.open("a") as f:
            f.write(f"{cube.path}\n")
