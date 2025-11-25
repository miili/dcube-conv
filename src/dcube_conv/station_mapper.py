from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import fiona
from pydantic import BaseModel, Field, FilePath, PositiveFloat, PrivateAttr

from dcube_conv.model import Location

if TYPE_CHECKING:
    from dcube_conv.stations import CubeSite

logger = logging.getLogger(__name__)


SensorID = str
ChannelID = str

ChannelMapping = dict[ChannelID, ChannelID]
DegreeFromNorth = float


DIP_MAP = {
    "N": 0.0,
    "E": 0.0,
    "Z": -90.0,
    "1": 0.0,
    "2": 0.0,
}

AZIMUTH_MAP = {
    "N": 0.0,
    "E": 90.0,
    "Z": 0.0,
    "1": 0.0,
    "2": 90.0,
}


class StationOrientationOverwrite(BaseModel):
    dip: float = 0.0
    azimuth: float = 0.0


class Station(Location):
    name: str
    seismic_sensor: str
    location: str = Field(default="", max_length=3)

    _parent: StationMapper | None = PrivateAttr(None)

    def set_parent(self, parent: StationMapper) -> None:
        self._parent = parent

    def get_channel_map(self) -> ChannelMapping:
        if not self._parent:
            raise RuntimeError(f"Station {self.name} has no parent.")
        return self._parent.get_channel_map(self.seismic_sensor, self.name)

    def get_channel_dip(self, channel: str) -> PositiveFloat:
        if not self._parent:
            raise RuntimeError(f"Station {self.name} has no parent.")
        return self._parent.get_channel_dip(self.seismic_sensor, self.name, channel)

    def get_channel_azimuth(self, channel: str) -> PositiveFloat:
        if not self._parent:
            raise RuntimeError(f"Station {self.name} has no parent.")
        return self._parent.get_channel_azimuth(self.seismic_sensor, self.name, channel)

    def has_orientation_overwrite(self) -> bool:
        if not self._parent:
            raise RuntimeError(f"Station {self.name} has no parent.")
        return self._parent.has_orientation_overwrite(self.name)

    @classmethod
    def from_feature(cls, feature: dict[str, Any]) -> Self:
        return cls(
            lat=feature["geometry"]["coordinates"][1],
            lon=feature["geometry"]["coordinates"][0],
            elevation=feature["geometry"]["coordinates"][2],
            depth=feature["properties"].get("depth", 0.0),
            name=feature["properties"]["station_name"],
            seismic_sensor=feature["properties"]["seismic_sensor"],
            # location=feature["properties"].get("location", "") or "",
        )


class StationMapper(BaseModel):
    geopackage: FilePath = FilePath("stations.gpkg")
    distance_threshold: PositiveFloat = 25.0
    channel_map: dict[SensorID, ChannelMapping] = {
        "4.5hz": {
            "p0": "EPZ",
            "p1": "EPN",
            "p2": "EPE",
        },
        "mark": {
            "p0": "EHZ",
            "p1": "EHN",
            "p2": "EHE",
        },
        "bb": {
            "p0": "HHZ",
            "p1": "HHN",
            "p2": "HHE",
        },
    }
    station_orientation_overwrites: dict[str, StationOrientationOverwrite] = {}

    _features: list[Station] = PrivateAttr([])

    def get_channel_map(self, sensor: SensorID, station: str) -> ChannelMapping:
        sensor_map = self.channel_map[sensor].copy()
        if station in self.station_orientation_overwrites:
            for in_channel, out_channel in sensor_map.items():
                if out_channel.endswith("N"):
                    out_channel = f"{out_channel[:-1]}1"
                elif out_channel.endswith("E"):
                    out_channel = f"{out_channel[:-1]}2"
                sensor_map[in_channel] = out_channel
        return sensor_map

    def get_channel_azimuth(
        self, sensor: SensorID, station: str, channel: str
    ) -> float:
        try:
            azimuth = AZIMUTH_MAP[channel[-1]]
        except KeyError as exc:
            raise ValueError(f"Unknown channel azimuth: {channel}") from exc
        if station in self.station_orientation_overwrites and not channel.endswith("Z"):
            overwrite = self.station_orientation_overwrites[station]
            azimuth += overwrite.azimuth

        return (azimuth + 360.0) % 360.0

    def has_orientation_overwrite(self, station: str) -> bool:
        return station in self.station_orientation_overwrites

    def get_channel_dip(self, sensor: SensorID, station: str, channel: str) -> float:
        try:
            return DIP_MAP[channel[-1]]
        except KeyError as exc:
            raise ValueError(f"Unknown channel dip: {channel}") from exc

    def prepare(self):
        check_geopackage(self.geopackage)
        self.load_geopackage(self.geopackage)

    def load_geopackage(self, file: Path) -> None:
        logger.info("Loading geopackage %s", file)
        with fiona.open(file) as data:
            for feature in data:
                station = Station.from_feature(feature)
                station.set_parent(self)
                self._features.append(station)

    def get_station(self, site: CubeSite) -> Station | None:
        distances = [feature.surface_distance_to(site) for feature in self._features]
        min_distance = min(distances)
        if min_distance > self.distance_threshold:
            return None

        station = self._features[distances.index(min_distance)]
        station.set_parent(self)
        return station


def check_geopackage(file: Path) -> bool:
    result = True
    with fiona.open(file) as data:
        for feature in data:
            properties = feature["properties"]
            id = feature["id"]
            if properties.get("status") == "planned":
                continue
            if not properties.get("station_name"):
                logger.error("Feature %s has no station_name", id)
                result = False
            if not properties.get("seismic_sensor"):
                logger.error("Feature %s has no seismic_sensor", id)
                result = False
            if not (geometry := feature.get("geometry")):
                logger.error("Feature %s has no geometry", id)
                result = False
            if (
                math.isnan(geometry["coordinates"][0])
                or math.isnan(geometry["coordinates"][1])
                or math.isnan(geometry["coordinates"][2])
            ):
                logger.error("Feature %s has nan geometry", id)
                result = False
            if geometry["coordinates"][2] == 0.0:
                logger.error("Feature %s has no elevation", id)
                result = False
    return result
