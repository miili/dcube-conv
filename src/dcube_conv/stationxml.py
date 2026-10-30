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
from obspy.core.inventory.response import Response, ResponseStage
from obspy.core.inventory.station import Station
from obspy.core.inventory.util import (
    Comment,
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
    from dcube_conv.stations import CubeLocations

_NRL_INSTANCE = None
NRLDatalogger = tuple[str, str, str]
NRLSensor = tuple[str, str, str, str] | tuple[str, str, str]


logger = logging.getLogger(__name__)


COMMENT_ORIENTATION = Comment(
    "Misoriented station, horizontal channel's azimuths were determined by AutoStatsQ.",
    authors=[Person(names=["Marius Isken", "Gesa Petersen"], agencies=["GFZ Potsdam"])],
)


def fix_units(response_stages: list[ResponseStage]):
    for stage in response_stages:
        if stage.input_units == "M/S":
            stage.input_units = "m/s"
        if stage.input_units == "COUNTS":
            stage.input_units = "count"
        if stage.output_units == "COUNTS":
            stage.output_units = "count"
        if stage.input_units == "COUNT":
            stage.input_units = "count"
        if stage.output_units == "COUNT":
            stage.output_units = "count"


class StationResponse(BaseModel):
    sensor_name: SensorID
    sensor: NRLSensor = ("SensorNederland", "PE-6", "375", "None")
    datalogger: NRLDatalogger = ("DiGOS/Omnirecs", "DATACUBE", "64")
    sensor_description: str
    extra_analog_gain: float | None = None

    def get_response(self, sampling_rate: int) -> Response:
        response = get_response(self.sensor, self.datalogger, sampling_rate)
        response = deepcopy(response)
        if self.extra_analog_gain:
            response.response_stages[1].stage_gain *= self.extra_analog_gain
            response.recalculate_overall_sensitivity()
        fix_units(response.response_stages)
        return response

    def get_datalogger_equipment(self, serial_number: str) -> Equipment:
        return Equipment(
            manufacturer=self.datalogger[0],
            model=self.datalogger[1],
            serial_number=serial_number,
            description="ADC with 24 bit and 3 channels",
        )

    def get_sensor_equipment(self) -> Equipment:
        return Equipment(
            manufacturer=self.sensor[0],
            model=self.sensor[1],
            description=self.sensor_description,
        )


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
    source: str = "GFZ Helmholtz-Centre for Geosciences"
    agency: str = "GFZ Helmholtz-Centre for Geosciences"

    country: str = "DE"
    region: str = "Western Bohemia"
    end_time: datetime = datetime(2023, 9, 1, tzinfo=timezone.utc)
    description: str = "ELISE - Eger Large-N Seismic Network"

    station_responses: dict[SensorID, StationResponse] = {
        "geophone": StationResponse(
            sensor_name="4.5hz",
            sensor=(
                "SensorNederland",
                "PE-6",
                "375",
                "None",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "64"),
            sensor_description="4.5 Hz geophone",
        ),
        "mark": StationResponse(
            sensor_name="mark",
            sensor=(
                "Sercel/Mark Products",
                "L-4C",
                "5500 Ohms",
                "8905 Ohms",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "8"),
            sensor_description="Mark L-4C short-period geophone",
        ),
        "bb": StationResponse(
            sensor_name="bb",
            sensor=(
                "Nanometrics",
                "Trillium Compact 120 (Vault, Posthole, OBS)",
                "750 V/m/s",
            ),
            datalogger=("DiGOS/Omnirecs", "DATACUBE", "8"),
            sensor_description="Broadband seismometer",
            extra_analog_gain=0.1,
        ),
    }

    def get_inventory(self, sites: CubeLocations) -> Inventory:
        sites.fill_endtimes(self.end_time)
        network_start_date = datetime.max.replace(tzinfo=timezone.utc)

        network = Network(
            code=sites.network,
            description=self.description,
            start_date=UTCDateTime(network_start_date),
            end_date=UTCDateTime(self.end_time),
            operators=[
                Operator(
                    agency=self.agency,
                    contacts=[
                        Person(
                            names=["Marius Paul Isken"],
                            agencies=[self.agency],
                            emails=["marius.isken@gfz.de"],
                        ),
                        Person(
                            names=["Torsten Dahm"],
                            agencies=[self.agency],
                            emails=["dahm@gfz.de"],
                        ),
                        Person(
                            names=["Jana Doubravová"],
                            agencies=[
                                "Institute of Geophysics, Czech Academy of Sciences"
                            ],
                            emails=["doubravka@ig.cas.cz"],
                        ),
                    ],
                    website="https://gfz.de",
                )
            ],
        )

        for site in sites.iter_stations():
            if not site.has_valid_elevation():
                logger.warning(
                    "Skipping: No elevation for cube %s (%f, %f)",
                    site.cube_id,
                    site.lat,
                    site.lon,
                )
                continue

            network_start_date = min(network_start_date, site.start_time)
            equipment = self.station_responses[site.station.seismic_sensor]
            datalogger = equipment.get_datalogger_equipment(site.cube_id)
            sensor = equipment.get_sensor_equipment()
            response = equipment.get_response(int(site.sampling_rate))

            is_misaligned = (
                site.station is not None and site.station.has_orientation_overwrite()
            )

            station = Station(
                code=site.station_name,
                latitude=Latitude(value=site.lat, datum="EPSG:4326"),
                longitude=Longitude(value=site.lon, datum="EPSG:4326"),
                elevation=float(f"{site.elevation:.1f}"),
                creation_date=UTCDateTime(site.start_time),
                start_date=UTCDateTime(site.start_time),
                end_date=UTCDateTime(site.end_time),
                comments=[COMMENT_ORIENTATION] if is_misaligned else None,
                site=Site(
                    name=site.station_name,
                    description=site.station.description or "unknown",
                    country=self.country,
                    region=self.region,
                ),
                equipments=[datalogger, sensor],
            )

            if site.station is None:
                continue

            for channel_name in site.station.get_channel_map().values():
                channel = Channel(
                    code=channel_name,
                    location_code=site.location_code,
                    latitude=Latitude(station.latitude, datum="EPSG:4326"),
                    longitude=Longitude(station.longitude, datum="EPSG:4326"),
                    elevation=round(site.elevation, 1),
                    depth=site.depth,
                    sample_rate=site.sampling_rate,
                    start_date=UTCDateTime(site.start_time),
                    end_date=UTCDateTime(site.end_time),
                    azimuth=site.station.get_channel_azimuth(channel_name),
                    dip=site.station.get_channel_dip(channel_name),
                )
                channel.response = response
                station.channels.append(channel)

            network.stations.append(station)

        network.start_date = UTCDateTime(network_start_date)

        inventory = Inventory(
            source=self.source,
            module="DataCube Raid",
            module_uri="https://pyrocko.org",
        )
        inventory.networks.append(network)
        return inventory

    def dump_stationxml(self, sites: CubeLocations, file: Path):
        logger.info("Dumping StationXML to %s", file)
        inv = self.get_inventory(sites)
        n_stations = sum(len(net.stations) for net in inv.networks)
        logger.info("Dumped %d stations to StationXML", n_stations)
        inv.write(file, format="STATIONXML", validate=True)
        self.dump_missing_stations_csv(sites, file.with_suffix(".missing.csv"))

    def dump_missing_stations_csv(self, sites: CubeLocations, file: Path):
        logger.info("Dumping missing stations to %s", file)
        with file.open("w") as f:
            f.write(
                "cube_id,location,lat,lon,elevation,depth,start_time,station_name,WKT_geom\n"
            )
            for s in sites.iter_sites():
                if s.station is None:
                    f.write(
                        f"{s.cube_id},{s.location_code},{s.lat},{s.lon},"
                        f"{s.elevation},{s.depth},{s.start_time},"
                        f"{s.station_name},{s.wkt_geom}\n"
                    )
