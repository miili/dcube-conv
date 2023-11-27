from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import fiona
from pydantic import BaseModel, FilePath, PositiveFloat, PrivateAttr

from dcube_conv.model import Location

if TYPE_CHECKING:
    from dcube_conv.stations import CubeSite

logger = logging.getLogger(__name__)


SensorID = str
ChannelID = str

ChannelMapping = dict[ChannelID, ChannelID]


class Station(Location):
    name: str
    seismic_sensor: str

    _parent: StationMapper | None = PrivateAttr(None)

    def set_parent(self, parent: StationMapper) -> None:
        self._parent = parent

    def get_channel_map(self) -> ChannelMapping:
        if not self._parent:
            raise RuntimeError("Station has no parent.")
        return self._parent.channel_map[self.seismic_sensor]

    @classmethod
    def from_feature(cls, feature: dict[str, Any]) -> Self:
        return cls(
            lat=feature["geometry"]["coordinates"][1],
            lon=feature["geometry"]["coordinates"][0],
            elevation=feature["geometry"]["coordinates"][2],
            depth=feature["properties"].get("depth", 0.0),
            name=feature["properties"]["station_name"],
            seismic_sensor=feature["properties"]["seismic_sensor"],
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

    _features: list[Station] = PrivateAttr([])

    def prepare(self):
        check_geopackage(self.geopackage)
        self.load_geopackage(self.geopackage)

    def load_geopackage(self, file: Path) -> None:
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
        return self._features[distances.index(min_distance)]


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
