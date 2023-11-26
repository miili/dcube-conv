import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Iterator

from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    PositiveInt,
    PrivateAttr,
    computed_field,
)
from pyrocko.io import datacube

from dcube_conv.model import DatacubeTraces, RecordLength, SteimCompression
from dcube_conv.utils import human_readable_bytes

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class LoadingStats(BaseModel):
    n_files: int = 0
    size_bytes_total: int = 0
    bytes_loaded: int = 0
    time_started: datetime = Field(default_factory=datetime.now)

    @computed_field
    def time_elapsed(self) -> timedelta:
        return datetime.now() - self.time_started

    @computed_field
    @property
    def loading_rate(self) -> float:
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
    def percent(self) -> float:
        return self.bytes_loaded / self.size_bytes_total * 100


class LoaderDataCube(BaseModel):
    path: DirectoryPath = DirectoryPath(".")

    _stats: LoadingStats = PrivateAttr(default_factory=LoadingStats)
    _files: list[Path] = PrivateAttr(default_factory=list)

    def _iter_datacube_files(self) -> Iterator[Path]:
        logger.info("Scanning %s", self.path)
        for file in self.path.rglob("*.*"):
            with file.open("rb") as f:
                if datacube.detect(f.read(512)):
                    yield file

    def scan(self) -> None:
        for file in self._iter_datacube_files():
            self._stats.n_files += 1
            self._stats.size_bytes_total += file.stat().st_size
            self._files.append(file)
        print(self._stats)

    def __iter__(self) -> Iterator[Path]:
        return iter(self._files)

    async def iter_datacube(self) -> AsyncIterator[DatacubeTraces]:
        queue = asyncio.Queue(maxsize=20)
        iterator = iter(self)

        async def worker(file: Path) -> None:
            await queue.put(await asyncio.to_thread(DatacubeTraces.from_file, file))
            self._stats.bytes_loaded += file.stat().st_size

        async def fetch(n_files: int = 1) -> None:
            logger.info("Fetching %d files", n_files)
            async with asyncio.TaskGroup() as tg:
                for _ in range(n_files):
                    file = next(iterator, None)
                    if file is None:
                        await queue.put(None)
                        break
                    tg.create_task(worker(file))

        async with asyncio.TaskGroup() as tg:
            tg.create_task(fetch(queue.maxsize))

            while True:
                tg.create_task(fetch(1))
                datacube = await queue.get()
                if datacube is None:
                    break
                yield datacube
                queue.task_done()
                print(
                    human_readable_bytes(self._stats.loading_rate),
                    self._stats.time_remaining,
                )


class Converter(BaseModel):
    input: LoaderDataCube = LoaderDataCube()
    output_path: DirectoryPath = DirectoryPath(".")
    output_template: str = "%(network)s/%(station)s/%(location)s/%(channel)s.mseed"

    record_length: RecordLength = 4096
    steim_compression: SteimCompression = 1

    write_threads: PositiveInt = 20

    async def _async_save(self, datacube: AsyncIterator[DatacubeTraces]) -> None:
        limit = asyncio.Semaphore(self.write_threads)

        async def worker(datacube: DatacubeTraces) -> None:
            async with limit:
                await asyncio.to_thread(
                    datacube.save,
                    self.output_path / self.output_template,
                    record_length=self.record_length,
                    steim=self.steim_compression,
                )

        async with asyncio.TaskGroup() as tg:
            async for dc in datacube:
                tg.create_task(worker(dc))

    async def convert(self) -> None:
        self.input.scan()
        logger.info(
            "Found %d datacube files (%.2f GB)",
            self.input._stats.n_files,
            self.input._stats.size_bytes_total / 1e9,
        )
        await self._async_save(self.input.iter_datacube())
