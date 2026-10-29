from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import TYPE_CHECKING

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


class StationMapper(BaseModel):
    geopackage: FilePath = FilePath("stations.gpkg")
    distance_threshold_meters: PositiveFloat = 25.0
    sensor_field: str = "sensor"
    station_field: str = "site_id"
    channel_map: dict[SensorID, ChannelMapping] = {
        "geophone": {
            "p0": "EPZ",
            "p1": "EPN",
            "p2": "EPE",
        },
        "short-period": {
            "p0": "EHZ",
            "p1": "EHN",
            "p2": "EHE",
        },
        "broadband": {
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

    def prepare(self) -> None:
        self.check_geopackage(self.geopackage)
        self.load_geopackage(self.geopackage)

    def load_geopackage(self, file: Path) -> None:
        logger.info("Loading GeoPackage %s", file)
        with fiona.open(file) as data:
            logger.info("Loaded %d features", len(data))
            for feature in data:
                station = self.new_station(feature)
                self._features.append(station)

    def new_station(self, feature) -> Station:
        try:
            elevation = feature["geometry"]["coordinates"][2]
        except IndexError:
            elevation = 0.0
        if not (name := feature["properties"][self.station_field]):
            raise ValueError(f"Station feature {feature['id']} has no name.")

        station = Station(
            lat=feature["geometry"]["coordinates"][1],
            lon=feature["geometry"]["coordinates"][0],
            depth=feature["properties"].get("depth", 0.0),
            elevation=elevation,
            name=name,
            seismic_sensor=feature["properties"][self.sensor_field],
            # location=feature["properties"].get("location", "") or "",
        )
        station.set_parent(self)
        return station

    def get_station(self, site: CubeSite) -> Station | None:
        distances = [feature.surface_distance_to(site) for feature in self._features]
        min_distance = min(distances)
        if min_distance > self.distance_threshold_meters:
            return None

        station = self._features[distances.index(min_distance)]
        station.set_parent(self)
        return station

    def check_geopackage(self, file: Path) -> None:
        result = True
        with fiona.open(file) as data:
            for feature in data:
                properties = feature["properties"]

                id = feature["id"]
                if properties.get("status") == "not_installed":
                    continue
                if not properties.get(self.station_field):
                    logger.error("Feature %s has no station_name", id)
                    result = False
                if not properties.get(self.sensor_field):
                    logger.error("Feature %s has no seismic_sensor", id)
                    result = False
                if not (geometry := feature.get("geometry")):
                    logger.error("Feature %s has no geometry", id)
                    result = False
                if (
                    math.isnan(geometry["coordinates"][0])
                    or geometry["coordinates"][0] == 0.0
                    or math.isnan(geometry["coordinates"][1])
                    or geometry["coordinates"][1] == 0.0
                ):
                    logger.error("Feature %s has bad geometry", id)
                    result = False
                # if geometry["coordinates"][2] == 0.0:
                #     logger.error("Feature %s has no elevation", id)
                #     result = False

        if not result:
            raise RuntimeError(f"GeoPackage {file} failed validation.")
