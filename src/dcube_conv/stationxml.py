from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from obspy.clients.nrl.client import NRL
from obspy.core.inventory.channel import Channel
from obspy.core.inventory.inventory import Inventory
from obspy.core.inventory.network import Network
from obspy.core.inventory.station import Station
from obspy.core.inventory.util import (
    Comment,
    Equipment,
    Latitude,
    Longitude,
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


logger = logging.getLogger(__name__)


@lru_cache
def get_response(
    sensor: NRLSensor,
    datalogger: NRLDatalogger,
    sampling_rate: int = 100,
    gain: float = 1.0,
):
    global _NRL_INSTANCE
    if _NRL_INSTANCE is None:
        _NRL_INSTANCE = NRL()

    print(datalogger, sensor, sampling_rate)

    response = _NRL_INSTANCE.get_response(
        sensor_keys=sensor,
        datalogger_keys=(*datalogger, str(sampling_rate)),
    )
    response.response_stages[1].gain = gain  # Breakoutbox
    response.recalculate_overall_sensitivity()
    return response


class StationXML(BaseModel):
    source: str = "GFZ Potsdam"
    description: str = "Eifel Large-N Network."
    cube_response: dict[SensorID, NRLDatalogger] = {
        "4.5hz": ("DiGOS/Omnirecs", "DATACUBE", "64"),
        "mark": ("DiGOS/Omnirecs", "DATACUBE", "8"),
        "bb": ("DiGOS/Omnirecs", "DATACUBE", "8"),
    }
    sensor_response: dict[SensorID, NRLSensor] = {
        "4.5hz": (
            "SensorNederland",
            "PE-6",
            "375",
            "None",  # Damping
        ),
        "mark": (
            "Sercel/Mark Products",
            "L-4C",
            "5500 Ohms",
            "None",
        ),  # TODO: Use GIPP restitions!
        "bb": (
            "Nanometrics",
            "Trillium Compact 20 (Vault, Posthole, OBS)",
            "1500 V/m/s",
        ),  # TODO: Add 1:10
        "bb_120": (
            "Nanometrics",
            "Trillium Compact 120 (Vault, Posthole, OBS)",
            "1500 V/m/s",
        ),  # TODO: Add 1:10
    }

    def get_inventory(self, sites: CubeSites) -> Inventory:
        sites.fill_endtimes()
        start_date = datetime.max.replace(tzinfo=timezone.utc)

        network = Network(
            code=sites.network,
            start_date=UTCDateTime(start_date),
        )

        for site in sites.iter_sites():
            start_date = min(start_date, site.start_time)

            station = Station(
                code=site.station_name,
                latitude=Latitude(value=site.lat, datum="EPSG:4326"),
                longitude=Longitude(value=site.lon, datum="EPSG:4326"),
                elevation=site.elevation,
                creation_date=UTCDateTime(site.start_time),
                site=Site(
                    description=Comment(site.cube_id),
                    country="DE",
                    region="Eifel",
                ),
            )

            sensor = None
            response = None
            if site.station is not None:
                manufatur, model, *_ = self.sensor_response[site.station.seismic_sensor]
                sensor = Equipment(
                    manufacturer=manufatur,
                    model=model,
                    serial_number=site.cube_id,
                )
                response = get_response(
                    self.sensor_response[site.station.seismic_sensor],
                    self.cube_response[site.station.seismic_sensor],
                    int(site.sampling_rate),
                )
            for channel_name in site.channel_map.values():
                channel = Channel(
                    code=channel_name,
                    location_code=site.location,
                    latitude=station.latitude,
                    longitude=station.longitude,
                    elevation=site.elevation,
                    depth=site.depth,
                    sample_rate=site.sampling_rate,
                    start_date=UTCDateTime(site.start_time),
                    end_date=UTCDateTime(site.end_time),
                    data_logger=Equipment(
                        manufacturer="DiGOS",
                        model="DataCube",
                        serial_number=site.cube_id,
                    ),
                    sensor=sensor,
                )
                channel.response = response
                station.channels.append(channel)

            network.stations.append(station)

        network.start_date = UTCDateTime(start_date)

        inventory = Inventory()
        inventory.networks.append(network)
        return inventory

    def dump_stationxml(self, sites: CubeSites, file: Path):
        logger.info("Dumping StationXML to %s", file)
        self.get_inventory(sites).write(str(file), format="STATIONXML", validate=True)
