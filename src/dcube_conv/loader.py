from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, ClassVar, Iterator

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
from rich.console import RenderableType
from rich.progress import Progress

from dcube_conv.model import CubeFile, CubeId
from dcube_conv.stats import Stats, StatsTable, make_progress, percent, problem
from dcube_conv.utils import (
    DATACUBE_HEADER_SIZE,
    DataCubeHeader,
    SessionId,
    detect_datacube,
    format_bytes,
)

logger = logging.getLogger(__name__)
MB = 1_000_000


class LoadingStats(Stats):
    title: ClassVar[str] = "Files"

    scanning: bool = True
    n_scanned: int = 0
    bytes_scanned: int = 0
    n_copies: int = 0
    bytes_copies: int = 0
    n_done: int = 0
    bytes_done: int = 0
    n_files: int = 0
    i_files: int = 0
    n_files_failed: int = 0
    bytes_failed: int = 0
    size_bytes_total: int = 0
    bytes_loaded: int = 0
    time_started: datetime = Field(default_factory=datetime.now)

    _queue: asyncio.Queue | None = PrivateAttr(default=None)
    _bar: Progress = PrivateAttr(default_factory=make_progress)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._position = 0
        self._task = self._bar.add_task("Scanning", total=None, detail="")

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
    def processed_percent(self) -> float:
        if not self.size_bytes_total:
            return 0.0
        return self.bytes_loaded / self.size_bytes_total * 100

    def add_bytes(self, nbytes: int) -> None:
        if self.bytes_loaded == 0:
            self.time_started = datetime.now()  # noqa DTZ005
        self.bytes_loaded += nbytes

    def progress_bar(self) -> RenderableType:
        if self.scanning:
            self._bar.update(
                self._task, detail=f"{self.n_scanned:,} datacube files found"
            )
        else:
            self._bar.update(
                self._task,
                description="Converting",
                total=self.size_bytes_total or 1,
                completed=self.bytes_loaded,
                detail=f"{self.i_files:,}/{self.n_files:,} files"
                f" · {format_bytes(self.loading_rate)}/s",
            )
        return self._bar

    def _populate(self, table: StatsTable) -> None:
        table.row(
            "found",
            self.n_scanned,
            size=self.bytes_scanned,
            detail=f"{self.n_copies:,} copies skipped"
            f" · {format_bytes(self.bytes_copies)}",
        )
        table.row(
            "converted",
            self.n_done,
            size=self.bytes_done,
            detail=f"{percent(self.bytes_done, self.bytes_scanned)} of found,"
            " in earlier runs",
        )
        table.row(
            "to convert",
            self.n_files,
            size=self.size_bytes_total,
            detail=f"{self.i_files - self.n_files_failed:,} loaded"
            f" · {problem(self.n_files_failed, 'failed', 'red')}"
            f" · queue {self.queue_size}/{self.queue_maxsize}",
        )


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
    _sessions: defaultdict[SessionId, list[tuple[Path, DataCubeHeader]]] = PrivateAttr(
        default_factory=lambda: defaultdict(list)
    )

    _progress_file: Path | None = PrivateAttr(default=None)
    _done_paths: set[Path] = PrivateAttr(default_factory=set)

    _queue: asyncio.Queue[CubeFile | None] = PrivateAttr(
        default_factory=lambda: asyncio.Queue(maxsize=32)
    )

    async def _scan_datacube_files(
        self,
    ) -> AsyncIterator[tuple[Path, DataCubeHeader, int]]:
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
                size = file.stat().st_size
                if size < self.min_file_size:
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

                yield file, header, size
                if ifile % 25 == 0:
                    await asyncio.sleep(0)  # Yield to event loop

    async def prepare(self) -> None:
        self._queue = asyncio.Queue(maxsize=self.queue_size)
        stats = self._stats

        recordings: defaultdict[tuple, list[tuple[Path, DataCubeHeader, int]]] = (
            defaultdict(list)
        )
        async for file, header, size in self._scan_datacube_files():
            recordings[header.recording_id].append((file, header, size))
            stats.n_scanned += 1
            stats.bytes_scanned += size

        for copies in recordings.values():
            file, header, size = max(copies, key=lambda copy: copy[2])
            if len(copies) > 1:
                stats.n_copies += len(copies) - 1
                stats.bytes_copies += sum(copy[2] for copy in copies) - size
                logger.debug(
                    "Duplicate recording in %s", ", ".join(str(c[0]) for c in copies)
                )
            self._sessions[header.session_id].append((file, header))
            # Also skips copies of converted files, they would be appended twice.
            if self._done_paths.intersection(copy[0] for copy in copies):
                stats.n_done += 1
                stats.bytes_done += size
                continue

            self._files.add(file)
            stats.n_files += 1
            stats.size_bytes_total += size

        for session in self._sessions.values():
            session.sort(key=lambda recording: recording[1].file_number)
        stats.scanning = False
        logger.info(
            "Found %d datacube files in %d sessions, %d to convert (%s)",
            len(recordings),
            len(self._sessions),
            stats.n_files,
            ByteSize(stats.size_bytes_total).human_readable(decimal=True),
        )

    @property
    def sessions(self) -> dict[SessionId, list[tuple[Path, DataCubeHeader]]]:
        """All recordings in range per session, including converted ones."""
        return self._sessions

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
                    size = file.stat().st_size
                    if cube is None:
                        stats.n_files_failed += 1
                        stats.bytes_failed += size
                    else:
                        await out_queue.put(cube)
                    stats.add_bytes(size)
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
