from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Self

import pyrocko.orthodrome as od
from pydantic import BaseModel, computed_field
from pyrocko.io import datacube, save
from pyrocko.trace import Trace

logger = logging.getLogger(__name__)

SteimCompression = Literal[1, 2]
RecordLength = Literal[512, 1024, 2048, 4096, 8192]
CubeId = str


@dataclass
class CubeTraces:
    path: Path
    traces: list[Trace]
    gps_tags: Any

    @computed_field
    @property
    def cube_id(self) -> CubeId:
        return self.path.suffix.lstrip(".").upper()

    @computed_field
    @property
    def start_time(self) -> datetime:
        return datetime.fromtimestamp(float(self.traces[0].tmin), tz=timezone.utc)

    @computed_field
    @property
    def end_time(self) -> datetime:
        return datetime.fromtimestamp(float(self.traces[0].tmax), tz=timezone.utc)

    @computed_field
    @property
    def sampling_rate(self) -> float:
        return float(1.0 / self.traces[0].deltat)

    @classmethod
    def from_file(cls, file: Path) -> Self | None:
        logger.debug("Loading %s", file)
        try:
            data = list(datacube.iload(str(file), yield_gps_tags=True))
        except Exception as e:
            logger.error("Failed to load %s: %s", file, e)
            return None
        gps_tags = data[0][1]
        return cls(path=file, traces=[tr for (tr, _) in data], gps_tags=gps_tags)

    def set_nsl(self, network: str, station: str, location: str) -> None:
        for trace in self.traces:
            trace.set_codes(network=network, station=station, location=location)

    def rename_channels(self, old: str, new: str) -> None:
        for trace in [tr for tr in self.traces if tr.channel == old]:
            trace.set_channel(new)

    def save(
        self,
        output_path: Path,
        record_length: RecordLength = 4096,
        steim: SteimCompression = 2,
    ) -> int:
        files = save(
            self.traces,
            str(output_path),
            record_length=record_length,
            steim=steim,
            append=True,
        )
        return sum(Path(f).stat().st_size for f in files)


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
