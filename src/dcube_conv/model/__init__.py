from __future__ import annotations

import asyncio
import logging
import math
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Self
from weakref import WeakValueDictionary

import pyrocko.orthodrome as od
from pydantic import BaseModel
from pyrocko.io import save
from pyrocko.trace import Trace

from dcube_conv.utils import load_datacube

logger = logging.getLogger(__name__)

SteimCompression = Literal[1, 2]
RecordLength = Literal[512, 1024, 2048, 4096, 8192]
CubeId = str


# Locks are dropped once no writer holds or waits for them.
WRITE_LOCKS: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()


def get_lock(file: str) -> asyncio.Lock:
    lock = WRITE_LOCKS.get(file)
    if lock is None:
        lock = WRITE_LOCKS[file] = asyncio.Lock()
    return lock


def files_size(files: list[str]) -> int:
    return sum(Path(f).stat().st_size for f in files if Path(f).exists())


@dataclass
class CubeTraces:
    path: Path
    traces: list[Trace]
    gps_tags: Any

    @property
    def cube_id(self) -> CubeId:
        return self.path.suffix.lstrip(".").upper()

    @property
    def start_time(self) -> datetime:
        return datetime.fromtimestamp(float(self.traces[0].tmin), tz=timezone.utc)

    @property
    def end_time(self) -> datetime:
        return datetime.fromtimestamp(float(self.traces[0].tmax), tz=timezone.utc)

    @property
    def sampling_rate(self) -> float:
        return float(1.0 / self.traces[0].deltat)

    @classmethod
    def from_file(cls, file: Path) -> Self | None:
        logger.debug("Loading %s", file)
        try:
            traces, gps_tags = load_datacube(file)
        except (Warning, Exception) as warn:
            logger.warning("Failed to load %s: %s", file, warn)
            return None
        if not traces:
            logger.warning("No traces in %s", file)
            return None
        return cls(path=file, traces=traces, gps_tags=gps_tags)

    def set_nsl(self, network: str, station: str, location: str) -> None:
        for trace in self.traces:
            trace.set_codes(network=network, station=station, location=location)

    def rename_channels(self, old: str, new: str) -> None:
        for trace in [tr for tr in self.traces if tr.channel == old]:
            trace.set_channel(new)

    async def save(
        self,
        output_path: Path,
        record_length: RecordLength = 4096,
        steim: SteimCompression = 2,
        executor: ThreadPoolExecutor | None = None,
    ) -> int:
        tr_tmin = datetime.fromtimestamp(
            min(tr.tmin for tr in self.traces),
            tz=timezone.utc,
        )
        tr_max = datetime.fromtimestamp(
            max(tr.tmax for tr in self.traces),
            tz=timezone.utc,
        )
        tr_tmid = tr_tmin + (tr_max - tr_tmin) / 2
        loop = asyncio.get_running_loop()

        # Year and day of year of the same instant, so a cube spanning New
        # Year ends up in a consistent day file.
        additional = {
            "year": f"{tr_tmid.year}",
            "julian_day": f"{tr_tmid.timetuple().tm_yday:03d}",
        }
        files = sorted(
            {tr.fill_template(str(output_path), **additional) for tr in self.traces}
        )

        def write() -> int:
            size_before = files_size(files)
            save(
                self.traces,
                str(output_path),
                additional=additional,
                record_length=record_length,
                steim=steim,
                append=True,
            )
            return files_size(files) - size_before

        async with AsyncExitStack() as stack:
            # Appends to the same file must not interleave. Locking in sorted
            # order keeps cubes sharing several files from deadlocking.
            for file in files:
                await stack.enter_async_context(get_lock(file))
            return await loop.run_in_executor(executor, write)


class Location(BaseModel):
    lat: float
    lon: float
    elevation: float = 0.0
    depth: float = 0.0

    @property
    def effective_elevation(self) -> float:
        return self.elevation - self.depth

    def surface_distance_to(self, other: Location) -> float:
        """Compute surface distance [m] to other location object.

        Args:
            other (Location): The other location.

        Returns:
            float: The surface distance in [m].
        """
        return float(
            od.distance_accurate50m_numpy(self.lat, self.lon, other.lat, other.lon)[0]
        )

    def distance_to(self, other: Location) -> float:
        """Compute 3-dimensional distance [m] to other location object.

        Args:
            other (Location): The other location.

        Returns:
            float: The distance in [m].
        """
        sx, sy, sz = od.geodetic_to_ecef(self.lat, self.lon, self.effective_elevation)
        ox, oy, oz = od.geodetic_to_ecef(self.lat, self.lon, other.effective_elevation)

        return math.sqrt((sx - ox) ** 2 + (sy - oy) ** 2 + (sz - oz) ** 2)

    def is_close(
        self,
        other: Location,
        distance_threshold_meters: float = 25.0,
    ) -> bool:
        """Check if two stations are close to each other.

        Args:
            other (Station): The other station.
            distance_threshold (float, optional): The distance threshold in [m].
                Defaults to 50.

        Returns:
            bool: True if close, False otherwise.
        """
        return self.surface_distance_to(other) < distance_threshold_meters
