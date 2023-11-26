import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Iterator

from pydantic import BaseModel, DirectoryPath, PrivateAttr
from pyrocko.io import datacube

from dcube_conv.model import DatacubeTraces

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class InputStats(BaseModel):
    n_files: int = 0
    size_bytes: int = 0


class InputDataCube(BaseModel):
    path: DirectoryPath = DirectoryPath(".")

    _stats: InputStats = PrivateAttr(default_factory=InputStats)
    _files: list[Path] = PrivateAttr(default_factory=list)

    def _iter_datacube_files(self) -> Iterator[Path]:
        for dirpath, _, filenames in self.path.walk():
            for filename in filenames:
                file = dirpath / filename
                with file.open() as f:
                    if datacube.detect(f.read(512)):
                        yield file

    def scan(self) -> None:
        for file in self._iter_datacube_files():
            self._stats.n_files += 1
            self._stats.size_bytes += file.stat().st_size
            self._files.append(file)

    def __iter__(self) -> Iterator[Path]:
        return iter(self._files)

    async def iter_datacube(self) -> AsyncIterator[DatacubeTraces]:
        queue = asyncio.Queue(maxsize=20)
        iterator = iter(self)

        async def worker(file: Path) -> None:
            await queue.put(await asyncio.to_thread(DatacubeTraces.from_file, file))

        async def fetch(n_files: int = 1) -> None:
            logger.info("Fetching %d files", n_files)
            async with asyncio.TaskGroup() as tg:
                for _ in range(n_files):
                    file = next(iterator, None)
                    if file is None:
                        await queue.put(None)
                        break
                    tg.create_task(worker(file))

        while True:
            asyncio.create_task(fetch(queue.maxsize - queue.qsize()))
            datacube = await queue.get()
            if datacube is None:
                break
            yield datacube
            queue.task_done()


class Converter(BaseModel):
    input: InputDataCube = InputDataCube()
    output_path: DirectoryPath = DirectoryPath(".")

    async def _async_save(self, datacube: AsyncIterator[DatacubeTraces]) -> None:
        async def worker(datacube: DatacubeTraces) -> None:
            await asyncio.to_thread(datacube.save, self.output_path)

        async def save() -> None:
            async with asyncio.TaskGroup() as tg:
                async for dc in datacube:
                    tg.create_task(worker(dc))

        await save()

    async def convert(self) -> None:
        self.input.scan()
        logger.info(
            "Found %d datacube files (%.2f GB)",
            self.input._stats.n_files,
            self.input._stats.size_bytes / 1e9,
        )
        await self._async_save(self.input.iter_datacube())
