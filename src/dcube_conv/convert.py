import asyncio
import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, Self

from pydantic import (
    BaseModel,
    ByteSize,
    DirectoryPath,
    Field,
    PositiveInt,
    PrivateAttr,
    computed_field,
    field_validator,
)
from rich.table import Table

from dcube_conv.loader import DataCubeLoader
from dcube_conv.model import CubeFile, RecordLength, SteimCompression
from dcube_conv.stations import CubeLocations
from dcube_conv.stats import RuntimeStats, Stats
from dcube_conv.utils import format_bytes

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


warnings.filterwarnings("error", message="Extrapolating GPS time information")
warnings.filterwarnings("error", message="No usable GPS timestamps found.")
# warnings.filterwarnings("error", message="Small number of GPS tags found.")


class ConverterStats(Stats):
    bytes_written: int = 0
    time_started: datetime = datetime.now(timezone.utc)

    def _populate_table(self, table: Table) -> None:
        table.add_row(
            "Bytes written", ByteSize(self.bytes_written).human_readable(decimal=True)
        )
        table.add_row("Write rate", f"{format_bytes(self.write_rate)}/s")

    def add_bytes(self, nbytes: int) -> None:
        if self.bytes_written == 0:
            self.time_started = datetime.now(timezone.utc)
        self.bytes_written += nbytes

    @computed_field
    @property
    def write_rate(self) -> float:
        elapsed = (datetime.now(timezone.utc) - self.time_started).total_seconds()
        return self.bytes_written / elapsed


class Converter(BaseModel):
    loader: DataCubeLoader = Field(default_factory=DataCubeLoader)
    stations: CubeLocations = Field(default_factory=CubeLocations)

    output_path: DirectoryPath = DirectoryPath("./sds")
    output_template: str = (
        "%(year)s/%(network)s/%(station)s/%(channel)s.D"
        "/%(network)s.%(station)s.%(location)s.%(channel)s.D"
        ".%(year)s.%(julian_day)s"
    )

    record_length: RecordLength = 4096
    steim_compression: SteimCompression = 2

    write_threads: PositiveInt = 16

    _stats: ConverterStats = PrivateAttr(default_factory=ConverterStats)

    @field_validator("output_template")
    @classmethod
    def _check_template_dates(cls, template: str) -> str:
        if "%(tmin_year)s" in template and "%(julian_day)s" in template:
            logger.warning(
                "output_template mixes %(tmin_year)s (trace start) with"
                " %(julian_day)s (cube mid-time), cubes spanning New Year get"
                " the wrong year. Use %(year)s instead."
            )
        return template

    async def _async_save(self, datacube_traces: AsyncIterator[CubeFile]) -> None:
        executor = ThreadPoolExecutor(max_workers=self.write_threads)
        # Bounds the cubes held in memory, the loader queue then backs up.
        slots = asyncio.Semaphore(self.write_threads)

        async def worker(cube: CubeFile) -> None:
            try:
                nbytes = await cube.save(
                    self.output_path / self.output_template,
                    record_length=self.record_length,
                    steim=self.steim_compression,
                    executor=executor,
                )
                self._stats.add_bytes(nbytes)
                self.loader.add_done(cube)
            finally:
                slots.release()

        tasks: set[asyncio.Task[None]] = set()
        errors: list[BaseException] = []

        def on_done(task: asyncio.Task[None]) -> None:
            tasks.discard(task)
            if not task.cancelled() and (exc := task.exception()):
                logger.error("Failed to save data cube: %s", exc)
                errors.append(exc)

        async for cube in datacube_traces:
            await slots.acquire()
            if errors:
                slots.release()
                break
            task = asyncio.create_task(worker(cube))
            tasks.add(task)
            task.add_done_callback(on_done)

        # Let started writes finish, so no cube is written without being
        # recorded in the progress file.
        await asyncio.gather(*tasks, return_exceptions=True)
        if errors:
            raise ExceptionGroup("Failed to save data cubes", errors)
        logger.info("All done saving data cubes.")

    async def convert(self) -> None:
        loop = asyncio.get_running_loop()
        cpu_count = int(os.environ.get("SLURM_CPUS_PER_TASK", 0))
        cpu_count = cpu_count or os.cpu_count() or 8
        loop.set_default_executor(ThreadPoolExecutor(max_workers=cpu_count))
        logger.info("Using %d threads", cpu_count)

        live = asyncio.create_task(RuntimeStats.live_view())
        await self.stations.prepare()
        await self.loader.prepare()

        loader = self.loader.iter_datacubes()
        processor = self.stations.process_datacubes(loader)
        await self._async_save(processor)
        live.cancel()

    @classmethod
    def load(cls, file: Path) -> Self:
        converter = cls.model_validate_json(file.read_bytes())

        station_file = file.with_name(f"{file.stem}.stations.json")
        if station_file.exists():
            converter.stations = CubeLocations.load(station_file)
        else:
            converter.stations._dump_path = station_file

        converter.loader.set_progress_file(file.with_name(f"{file.stem}.progress"))
        return converter
