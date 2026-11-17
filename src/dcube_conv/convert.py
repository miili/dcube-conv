import asyncio
import logging
import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, ClassVar, Self

from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    PositiveInt,
    PrivateAttr,
    computed_field,
    field_validator,
)

from dcube_conv.loader import DataCubeLoader
from dcube_conv.model import CubeFile, RecordLength, SteimCompression
from dcube_conv.stations import Deployments, state_file
from dcube_conv.stats import RuntimeStats, Stats, StatsTable, percent
from dcube_conv.utils import format_bytes

logger = logging.getLogger(__name__)


warnings.filterwarnings("error", message="Extrapolating GPS time information")
warnings.filterwarnings("error", message="No usable GPS timestamps found.")
# warnings.filterwarnings("error", message="Small number of GPS tags found.")


class ConverterStats(Stats):
    title: ClassVar[str] = "Output"

    n_files_written: int = 0
    bytes_written: int = 0
    n_writing: int = 0
    n_writers: int = 0
    bytes_read: int = 0
    time_started: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._position = 2

    def _populate(self, table: StatsTable) -> None:
        table.row(
            "written",
            self.n_files_written,
            "bold green",
            size=self.bytes_written,
            detail=f"{format_bytes(self.write_rate)}/s"
            f" · {percent(self.bytes_written, self.bytes_read)} of input size",
        )
        table.row(
            "writers",
            f"{self.n_writing}/{self.n_writers}",
            detail="threads appending to SDS day files",
        )

    def add_bytes(self, nbytes: int) -> None:
        if self.bytes_written == 0:
            self.time_started = datetime.now(timezone.utc)
        self.bytes_written += nbytes
        self.n_files_written += 1

    @computed_field
    @property
    def write_rate(self) -> float:
        elapsed = (datetime.now(timezone.utc) - self.time_started).total_seconds()
        return self.bytes_written / elapsed if elapsed > 0 else 0.0


class Converter(BaseModel):
    loader: DataCubeLoader = Field(default_factory=DataCubeLoader)
    stations: Deployments = Field(default_factory=Deployments)

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
        self._stats.n_writers = self.write_threads
        # Bounds the cubes held in memory, the loader queue then backs up.
        slots = asyncio.Semaphore(self.write_threads)

        async def worker(cube: CubeFile) -> None:
            self._stats.n_writing += 1
            try:
                nbytes = await cube.save(
                    self.output_path / self.output_template,
                    record_length=self.record_length,
                    steim=self.steim_compression,
                    executor=executor,
                )
                self._stats.add_bytes(nbytes)
                self._stats.bytes_read += cube.size
                self.loader.add_done(cube)
            finally:
                self._stats.n_writing -= 1
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

    def _set_executor(self) -> None:
        loop = asyncio.get_running_loop()
        cpu_count = int(os.environ.get("SLURM_CPUS_PER_TASK", 0))
        cpu_count = cpu_count or os.cpu_count() or 8
        loop.set_default_executor(ThreadPoolExecutor(max_workers=cpu_count))
        logger.info("Using %d threads", cpu_count)

    async def convert(self) -> None:
        self._set_executor()
        live = asyncio.create_task(RuntimeStats.live_view())
        try:
            await self.loader.prepare()
            await self.stations.prepare(self.loader.sessions)

            cubes = self.loader.iter_datacubes()
            await self._async_save(self.stations.process_datacubes(cubes))
            self.log_summary()
        finally:
            await asyncio.sleep(0.3)  # Show the final state
            live.cancel()

    async def locate(self) -> None:
        """Locate the recording sessions and save the deployments.

        Scans the file headers and locates new and outdated sessions without
        converting any data, e.g. to update the deployments for StationXML.
        """
        self._set_executor()
        live = asyncio.create_task(RuntimeStats.live_view())
        try:
            await self.loader.prepare()
            await self.stations.prepare(self.loader.sessions)
            self.stations.save(views=True)
        finally:
            await asyncio.sleep(0.3)  # Show the final state
            live.cancel()

    def log_summary(self) -> None:
        files = self.loader._stats
        deployments = self.stations._stats
        logger.info(
            "Files: %d to convert, %d written, %d failed to load, %d without"
            " station, %d excluded; %d converted before, %d copies skipped",
            files.n_files,
            self._stats.n_files_written,
            files.n_files_failed,
            deployments.n_files_no_station,
            deployments.n_files_excluded,
            files.n_done,
            files.n_copies,
        )
        logger.info(
            "Data: %s to convert, read %s at %s/s, wrote %s at %s/s;"
            " %s converted before",
            format_bytes(files.size_bytes_total),
            format_bytes(files.bytes_loaded),
            format_bytes(files.loading_rate),
            format_bytes(self._stats.bytes_written),
            format_bytes(self._stats.write_rate),
            format_bytes(files.bytes_done),
        )
        logger.info(
            "Deployments: %d sessions, %d without GPS fix, %d without station,"
            " %d stations",
            deployments.n_sessions,
            deployments.n_no_gps,
            deployments.n_no_station,
            deployments.n_stations,
        )

    @classmethod
    def load(cls, file: Path) -> Self:
        converter = cls.model_validate_json(file.read_bytes())

        converter.stations.load_state(state_file(file))
        converter.loader.set_progress_file(file.with_name(f"{file.stem}.progress"))
        return converter
