from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from obspy.clients.nrl.client import NRL
from obspy.core.inventory.channel import Channel
from obspy.core.inventory.inventory import Inventory
from obspy.core.inventory.network import Network
from obspy.core.inventory.response import Response
from obspy.core.inventory.station import Station
from obspy.core.inventory.util import (
    Equipment,
    Latitude,
    Longitude,
    Operator,
    Person,
    Site,
)
from obspy.core.utcdatetime import UTCDateTime
from pydantic import BaseModel

from dcube_conv.station_mapper import SensorID

if TYPE_CHECKING:
    from dcube_conv.stations import CubeSites

_NRL_INSTANCE = None
NRLDatalogger = tuple[str, str, str]
NRLSensor = tuple[str, str, str, str] | tuple[str, str, str]

DIP_MAP = {
    "N": 0.0,
    "E": 0.0,
    "Z": 90.0,
}

AZIMUTH_MAP = {
    "N": 0.0,
    "E": 90.0,
    "Z": 0.0,
}


logger = logging.getLogger(__name__)


class StationResponse(BaseModel):
    sensor_name: SensorID
    sensor: NRLSensor = ("SensorNederland", "PE-6", "375", "None")
    datalogger: NRLDatalogger = ("DiGOS/Omnirecs", "DATACUBE", "64")
    extra_analog_gain: float | None = None

    def get_response(self, sampling_rate: int) -> Response:
        response = get_response(self.sensor, self.datalogger, sampling_rate)
        response = deepcopy(response)
        if self.extra_analog_gain:
            response.response_stages[1].stage_gain *= self.extra_analog_gain
            response.recalculate_overall_sensitivity()
        return response

    def get_datalogger_equipment(self, serial_number: str) -> Equipment:
        return Equipment(
            manufacturer=self.datalogger[0],
            model=self.datalogger[1],
            serial_number=serial_number,
        )

    def get_sensor_equipment(self) -> Equipment:
        return Equipment(manufacturer=self.sensor[0], model=self.sensor[1])


@lru_cache
def get_response(
    sensor: NRLSensor,
    datalogger: NRLDatalogger,
    sampling_rate: int,
) -> Response:
    global _NRL_INSTANCE
    _NRL_INSTANCE = _NRL_INSTANCE or NRL()

    return _NRL_INSTANCE.get_response(
        sensor_keys=sensor,
        datalogger_keys=(*datalogger, str(sampling_rate)),
    )


class StationXML(BaseModel):
    source: str = "GFZ Potsdam"
    country: str = "DE"
    region: str = "Eifel"
    end_time: datetime = datetime(2023, 9, 1, tzinfo=timezone.utc)
    description: str = "Eifel Large-N Seismic Network."

    station_responses: dict[SensorID, StationResponse] = {
        "4.5hz": StationResponse(
            sensor_name="4.5hz",
            sensor=(
                "SensorNederland",
                "PE-6",
                "375",
                "None",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "64"),
        ),
        "mark": StationResponse(
            sensor_name="mark",
            sensor=(
                "Sercel/Mark Products",
                "L-4C",
                "5500 Ohms",
                "None",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "8"),
        ),
        "bb": StationResponse(
            sensor_name="bb",
            sensor=(
                "Nanometrics",
                "Trillium Compact 20 (Vault, Posthole, OBS)",
                "1500 V/m/s",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "8"),
            extra_analog_gain=0.1,
        ),
    }

    def get_inventory(self, sites: CubeSites) -> Inventory:
        sites.fill_endtimes(self.end_time)
        start_date = datetime.max.replace(tzinfo=timezone.utc)

        network = Network(
            code=sites.network,
            description=self.description,
            start_date=UTCDateTime(start_date),
            end_date=UTCDateTime(self.end_time),
            operators=[
                Operator(
                    agency="GFZ Potsdam",
                    contacts=[
                        Person(
                            names=["Marius Paul Isken"],
                            agencies=["GFZ Potsdam"],
                            emails=["mi@gfz-potsdam.de"],
                        ),
                        Person(
                            names=["Torsten Dahm"],
                            agencies=["GFZ Potsdam"],
                            emails=["dahm@gfz-potsdam.de"],
                        ),
                        Person(
                            names=["Christoph Sens-Schönfelder"],
                            agencies=["GFZ Potsdam"],
                            emails=["sens-schoenfelder@gfz-potsdam.de"],
                        ),
                    ],
                    website="https://gfz-potsdam.de",
                )
            ],
        )

        for site in sites.iter_sites():
            if site.station is None:
                logger.warning(
                    "Skipping: No station for site %s (%f, %f)",
                    site.cube_id,
                    site.lat,
                    site.lon,
                )
                continue

            if not site.has_valid_elevation():
                logger.warning(
                    "Skipping: No elevation for site %s (%f, %f)",
                    site.cube_id,
                    site.lat,
                    site.lon,
                )
                continue

            start_date = min(start_date, site.start_time)

            station = Station(
                code=site.station_name,
                latitude=Latitude(value=site.lat, datum="EPSG:4326"),
                longitude=Longitude(value=site.lon, datum="EPSG:4326"),
                elevation=float(f"{site.elevation:.1f}"),
                creation_date=UTCDateTime(site.start_time),
                start_date=UTCDateTime(site.start_time),
                end_date=UTCDateTime(site.end_time),
                site=Site(
                    name=site.station_name,
                    description=site.station.location,
                    country=self.country,
                    region=self.region,
                ),
            )

            response = None
            datalogger = None
            sensor = None
            if site.station is not None:
                equipment = self.station_responses[site.station.seismic_sensor]
                datalogger = equipment.get_datalogger_equipment(site.cube_id)
                sensor = equipment.get_sensor_equipment()
                response = equipment.get_response(int(site.sampling_rate))

            for channel_name in site.channel_map.values():
                channel = Channel(
                    code=channel_name,
                    location_code=site.location,
                    latitude=Latitude(station.latitude, datum="EPSG:4326"),
                    longitude=Longitude(station.longitude, datum="EPSG:4326"),
                    elevation=float(f"{site.elevation:.1f}"),
                    depth=site.depth,
                    sample_rate=site.sampling_rate,
                    start_date=UTCDateTime(site.start_time),
                    end_date=UTCDateTime(site.end_time),
                    data_logger=datalogger,
                    sensor=sensor,
                    azimuth=AZIMUTH_MAP[channel_name[-1]],
                    dip=DIP_MAP[channel_name[-1]],
                )
                channel.response = response
                station.channels.append(channel)
            network.stations.append(station)

        network.start_date = UTCDateTime(start_date)

        inventory = Inventory(
            source="GFZ Potsdam",
            module="Pyrocko DataCube Raid",
            module_uri="https://pyrocko.org",
        )
        inventory.networks.append(network)
        return inventory

    def dump_stationxml(self, sites: CubeSites, file: Path):
        logger.info("Dumping StationXML to %s", file)
        inv = self.get_inventory(sites)
        inv.write(file, format="STATIONXML", validate=True)
