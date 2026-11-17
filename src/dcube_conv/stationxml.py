from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from statistics import mean
from typing import TYPE_CHECKING, Any

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
from pydantic import BaseModel, field_validator

from dcube_conv.model import Location
from dcube_conv.station_mapper import SensorID

if TYPE_CHECKING:
    from dcube_conv.stations import Deployment, Deployments

NRLKeys = tuple[str, ...]

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
    # The preamplifier stage of the NRL v2 DATA-CUBE responses has no units.
    for previous, stage in pairwise(response_stages):
        if stage.input_units is None:
            stage.input_units = previous.output_units
        if stage.output_units is None:
            stage.output_units = stage.input_units


@lru_cache
def get_nrl(root: Path) -> NRL:
    return NRL(str(root.expanduser()))


@lru_cache
def get_response(root: Path, sensor: NRLKeys, datalogger: NRLKeys) -> Response:
    try:
        return get_nrl(root).get_response(
            sensor_keys=list(sensor), datalogger_keys=list(datalogger)
        )
    except KeyError as exc:
        raise KeyError(
            f"NRL {root} has no response for sensor {sensor} and datalogger"
            f" {datalogger}, NRL v2 keys are required"
        ) from exc


class StationResponse(BaseModel):
    sensor_name: SensorID
    sensor: NRLKeys
    """NRL v2 sensor keys."""
    datalogger: NRLKeys = ("DiGOSOmnirecs", "DataCube")
    """NRL v2 datalogger keys, without gain and sampling rate."""
    sensor_description: str
    extra_analog_gain: float | None = None

    @field_validator("datalogger", mode="before")
    @classmethod
    def _drop_v1_datalogger(cls, value: Any) -> Any:
        # Old configs use NRL v1 keys including the gain, now taken per deployment.
        if tuple(value[:2]) == ("DiGOS/Omnirecs", "DATACUBE"):
            return ("DiGOSOmnirecs", "DataCube")
        return value

    def get_response(self, nrl_root: Path, sampling_rate: float, gain: int) -> Response:
        response = get_response(
            nrl_root,
            tuple(self.sensor),
            (*self.datalogger, str(gain), f"{round(sampling_rate)} Hz"),
        )
        response = deepcopy(response)
        fix_units(response.response_stages)
        if self.extra_analog_gain:
            response.response_stages[1].stage_gain *= self.extra_analog_gain
        # The NRL sensitivity is wrong when its stage units were incomplete.
        response.recalculate_overall_sensitivity()
        return response

    def get_datalogger_equipment(self, gain: int) -> Equipment:
        return Equipment(
            manufacturer=self.datalogger[0],
            model=self.datalogger[1],
            description=f"ADC with 24 bit and 3 channels, preamplifier gain {gain}",
        )

    def get_sensor_equipment(self) -> Equipment:
        return Equipment(
            manufacturer=self.sensor[0],
            model=self.sensor[1],
            description=self.sensor_description,
        )


class ChannelEpoch(Location):
    """Consecutive deployments of a station with the same response and place.

    Cubes are swapped and restarted at every service visit, each restart is a
    new deployment. The epoch only changes with the gain, the sampling rate or
    a move of the sensor. Its position is the mean of its deployments.
    """

    start_time: datetime
    end_time: datetime
    sampling_rate: float
    gain: int
    n_deployments: int

    @classmethod
    def from_deployments(cls, deployments: list[Deployment]) -> ChannelEpoch:
        first = deployments[0]
        return cls(
            lat=mean(d.lat for d in deployments),
            lon=mean(d.lon for d in deployments),
            elevation=mean(d.elevation for d in deployments),
            depth=mean(d.depth for d in deployments),
            start_time=min(d.start_time for d in deployments),
            end_time=max(d.end_time for d in deployments),
            sampling_rate=first.sampling_rate,
            gain=first.gain,
            n_deployments=len(deployments),
        )


def channel_epochs(
    deployments: list[Deployment], max_distance: float
) -> list[ChannelEpoch]:
    """Merge the deployments of a station into channel epochs.

    Args:
        deployments (list[Deployment]): Deployments of one station.
        max_distance (float): Largest distance of a deployment to the mean
            position of the epoch it joins, in [m].

    Returns:
        list[ChannelEpoch]: The epochs, sorted by time and not overlapping.
    """
    groups: list[list[Deployment]] = []
    for deployment in sorted(deployments, key=lambda d: d.start_time):
        if groups:
            group = groups[-1]
            same_response = (group[0].gain, group[0].sampling_rate) == (
                deployment.gain,
                deployment.sampling_rate,
            )
            distance = ChannelEpoch.from_deployments(group).surface_distance_to(
                deployment
            )
            if same_response and distance <= max_distance:
                group.append(deployment)
                continue
        groups.append([deployment])

    epochs = [ChannelEpoch.from_deployments(group) for group in groups]
    # The next cube is often switched on before the last one is stopped.
    for epoch, following in pairwise(epochs):
        epoch.end_time = min(epoch.end_time, following.start_time)
    return epochs


class StationXML(BaseModel):
    source: str = "GFZ Helmholtz-Centre for Geosciences"
    agency: str = "GFZ Helmholtz-Centre for Geosciences"

    country: str = "DE"
    region: str = "Western Bohemia"
    description: str = "ELISE - Eger Large-N Seismic Network"

    nrl_root: Path = Path("~/.cache/NRL")
    """Local copy of the Nominal Response Library v2."""

    merge_distance_meters: float = 15.0
    """Deployments closer than this to an epoch's mean position join it."""

    station_responses: dict[SensorID, StationResponse] = {
        "geophone": StationResponse(
            sensor_name="geophone",
            sensor=("SensorNederland", "PE-6B", "4.5 s", "375", "None"),
            sensor_description="4.5 Hz geophone",
        ),
        "short-period": StationResponse(
            sensor_name="short-period",
            sensor=("Sercel", "L-4C", "1 Hz", "5500", "8905", "171"),
            sensor_description="Mark L-4C short-period geophone",
        ),
        "broadband": StationResponse(
            sensor_name="broadband",
            sensor=("Nanometrics", "TrilliumCompact120", "754"),
            sensor_description="Broadband seismometer",
            extra_analog_gain=0.1,
        ),
    }

    def get_inventory(self, deployments: Deployments) -> Inventory:
        network = Network(
            code=deployments.network,
            description=self.description,
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

        n_deployments = n_channel_epochs = 0
        for station in deployments.iter_stations():
            valid = [d for d in station.deployments if d.has_valid_elevation()]
            for deployment in station.deployments:
                if deployment not in valid:
                    logger.warning(
                        "Skipping: No elevation for cube %s at %s (%f, %f)",
                        deployment.cube_id,
                        station.code,
                        deployment.lat,
                        deployment.lon,
                    )
            if not valid:
                continue
            n_deployments += len(valid)

            equipment = self.station_responses[station.station.seismic_sensor]
            sensor = equipment.get_sensor_equipment()
            epochs = channel_epochs(valid, self.merge_distance_meters)
            n_channel_epochs += len(epochs)
            latest = epochs[-1]
            start_time = epochs[0].start_time
            end_time = max(e.end_time for e in epochs)

            xml_station = Station(
                code=station.code,
                latitude=Latitude(value=latest.lat, datum="EPSG:4326"),
                longitude=Longitude(value=latest.lon, datum="EPSG:4326"),
                elevation=round(latest.elevation, 1),
                creation_date=UTCDateTime(start_time),
                start_date=UTCDateTime(start_time),
                end_date=UTCDateTime(end_time),
                comments=[COMMENT_ORIENTATION]
                if station.station.has_orientation_overwrite()
                else None,
                site=Site(
                    name=station.code,
                    description=station.station.description or "unknown",
                    country=self.country,
                    region=self.region,
                ),
            )

            layout = station.station
            for epoch in epochs:
                response = equipment.get_response(
                    self.nrl_root, epoch.sampling_rate, epoch.gain
                )
                datalogger = equipment.get_datalogger_equipment(epoch.gain)
                for channel_code in layout.get_channel_map().values():
                    channel = Channel(
                        code=channel_code,
                        location_code=valid[-1].location_code,
                        latitude=Latitude(epoch.lat, datum="EPSG:4326"),
                        longitude=Longitude(epoch.lon, datum="EPSG:4326"),
                        elevation=round(epoch.elevation, 1),
                        depth=epoch.depth,
                        sample_rate=epoch.sampling_rate,
                        start_date=UTCDateTime(epoch.start_time),
                        end_date=UTCDateTime(epoch.end_time),
                        azimuth=layout.get_channel_azimuth(channel_code),
                        dip=layout.get_channel_dip(channel_code),
                        sensor=sensor,
                        data_logger=datalogger,
                    )
                    channel.response = response
                    xml_station.channels.append(channel)

            network.stations.append(xml_station)

        logger.info(
            "Merged %d deployments into %d channel epochs",
            n_deployments,
            n_channel_epochs,
        )
        if network.stations:
            network.start_date = min(s.start_date for s in network.stations)
            network.end_date = max(s.end_date for s in network.stations)

        inventory = Inventory(
            source=self.source,
            module="DataCube Raid",
            module_uri="https://pyrocko.org",
        )
        inventory.networks.append(network)
        return inventory

    def dump_stationxml(self, deployments: Deployments, file: Path):
        logger.info("Dumping StationXML to %s", file)
        inv = self.get_inventory(deployments)
        n_stations = sum(len(net.stations) for net in inv.networks)
        logger.info("Dumped %d stations to StationXML", n_stations)
        inv.write(file, format="STATIONXML", validate=True)
        self.dump_missing_stations_csv(deployments, file.with_suffix(".missing.csv"))

    def dump_missing_stations_csv(self, deployments: Deployments, file: Path):
        logger.info("Dumping deployments without station to %s", file)
        with file.open("w") as f:
            f.write("cube_id,session_start,start_time,end_time,lat,lon,WKT_geom\n")
            for d in deployments.iter_deployments():
                if d.station is None:
                    f.write(
                        f"{d.cube_id},{d.session_start},{d.start_time},"
                        f"{d.end_time},{d.lat},{d.lon},{d.wkt_geom}\n"
                    )
