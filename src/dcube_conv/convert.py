import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Self

from pydantic import (
    BaseModel,
    ByteSize,
    DirectoryPath,
    PositiveInt,
    PrivateAttr,
)
from rich.table import Table

from dcube_conv.loader import DataCubeLoader
from dcube_conv.model import CubeTraces, RecordLength, SteimCompression
from dcube_conv.stations import CubeSites
from dcube_conv.stats import RuntimeStats, Stats

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ConverterStats(Stats):
    bytes_written: int = 0

    def _populate_table(self, table: Table) -> None:
        table.add_row(
            "Bytes written", ByteSize(self.bytes_written).human_readable(decimal=True)
        )


class Converter(BaseModel):
    loader: DataCubeLoader = DataCubeLoader()
    stations: CubeSites = CubeSites()

    output_path: DirectoryPath = DirectoryPath("mseed")
    output_template: str = (
        "%(tmin_year)s/%(network)s/%(station)s/%(channel)s.D"
        "/%(network)s.%(station)s.%(location)s.%(channel)s.D"
        ".%(tmin_year)s.%(julianday)s"
    )

    record_length: RecordLength = 4096
    steim_compression: SteimCompression = 1

    write_threads: PositiveInt = 20

    _stats = PrivateAttr(default_factory=ConverterStats)

    async def _async_save(self, datacube_traces: AsyncIterator[CubeTraces]) -> None:
        limit = asyncio.Semaphore(self.write_threads + 1)

        async def worker(cube: CubeTraces) -> None:
            async with limit:
                bytes = await asyncio.to_thread(
                    cube.save,
                    self.output_path / self.output_template,
                    record_length=self.record_length,
                    steim=self.steim_compression,
                )
                self._stats.bytes_written += bytes
                self.loader.add_done(cube)

        async with asyncio.TaskGroup() as tg:
            async for dc in datacube_traces:
                async with limit:
                    tg.create_task(worker(dc))

    async def convert(self) -> None:
        loop = asyncio.get_running_loop()
        cpu_count = int(os.environ.get("SLURM_CPUS_PER_TASK", 0))
        cpu_count = cpu_count or os.cpu_count() or 8
        loop.set_default_executor(ThreadPoolExecutor(max_workers=cpu_count))
        logger.info("Using %d threads", cpu_count)

        live = asyncio.create_task(RuntimeStats.live_view())
        await self.loader.prepare()
        await self.stations.prepare()

        iterator = self.stations.process_datacubes(self.loader.iter_datacubes())
        await self._async_save(iterator)
        live.cancel()

    @classmethod
    def load(cls, file: Path) -> Self:
        converter = cls.model_validate_json(file.read_bytes())

        station_file = file.with_name(f"{file.stem}.stations.json")
        if station_file.exists():
            converter.stations = CubeSites.model_validate_json(station_file.read_text())
        converter.stations._dump_path = station_file

        converter.loader.set_progress_file(file.with_name(f"{file.stem}.progress"))
        return converter
