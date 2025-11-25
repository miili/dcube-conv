from __future__ import annotations

import asyncio
import logging
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, DefaultDict, Iterator

from pydantic import BaseModel, Field, PositiveFloat, PrivateAttr
from pyrocko.io.datacube import coordinates_from_gps
from pyrocko.model import Station as PyrockoStation
from pyrocko.model import dump_stations_yaml

from dcube_conv.model import CubeId, CubeTraces, Location
from dcube_conv.processors import ProcessorType
from dcube_conv.station_mapper import SensorID, Station, StationMapper
from dcube_conv.stats import Stats
from dcube_conv.utils import DATETIME_MAX, ElevationModel, get_elevation

if TYPE_CHECKING:
    from rich.table import Table

logger = logging.getLogger(__name__)


class SitesStats(Stats):
    n_sites: int = 0
    n_stations: int = 0
    n_no_site: int = 0

    def _populate_table(self, table: Table) -> None:
        table.add_row("Sites", str(self.n_sites))
        table.add_row("Sites not found", f"[red]{self.n_no_site}")
        table.add_row("Stations", str(self.n_stations))
        table.add_row("Stations not found", f"[red]{self.n_sites - self.n_stations}")


class CubeSite(Location):
    location: str = ""
    cube_id: str
    sampling_rate: PositiveFloat

    start_time: datetime
    end_time: datetime

    station: Station | None = None

    @property
    def station_name(self) -> str:
        return self.station.name if self.station else ""

    def set_station(self, station: Station) -> None:
        self.station = station

    @classmethod
    async def from_datacube_trace(cls, datacube: CubeTraces) -> CubeSite | None:
        lat, lon, elevation = await asyncio.to_thread(
            coordinates_from_gps, datacube.gps_tags
        )
        if math.isnan(lat) or math.isnan(lon) or lat == 0.0 or lon == 0.0:
            logger.error(
                "No GPS coordinates found for cube %s - %s.",
                datacube.cube_id,
                datacube.start_time,
            )
            return None
        return cls(
            cube_id=datacube.cube_id,
            lat=lat,
            lon=lon,
            elevation=elevation,
            sampling_rate=datacube.sampling_rate,
            start_time=datacube.start_time,
            end_time=datacube.end_time,
        )

    def update_start_time(self, start_time: datetime) -> None:
        self.start_time = min(self.start_time, start_time)

    def as_pyrocko_station(self, network: str = "") -> PyrockoStation:
        return PyrockoStation(
            network=network,
            station=self.station.name if self.station else self.cube_id,
            location=self.location,
            lat=self.lat,
            lon=self.lon,
            elevation=self.elevation,
        )

    def has_valid_elevation(self) -> bool:
        return not (self.elevation == -999999.0 or self.elevation == 0.0)

    async def query_elevation(self, model: ElevationModel = "aster30m") -> None:
        logger.debug("Querying elevation for %s", self)
        self.elevation = await get_elevation(self.lat, self.lon, model)


class CubeSites(BaseModel):
    network: str = Field(default="DC", max_length=2, pattern=r"[a-zA-Z0-9]")
    sites: DefaultDict[CubeId, list[CubeSite]] = Field(default_factory=defaultdict)
    elevation_model: ElevationModel = Field(default="aster30m")

    post_processors: dict[SensorID, list[ProcessorType]] = {}
    station_blacklist: set[str] = Field(default_factory=set)

    mapper: StationMapper | None = Field(default_factory=StationMapper)
    no_site_info: set[CubeId] = Field(default_factory=set)

    _dump_path: Path | None = PrivateAttr(None)
    _stats: SitesStats = PrivateAttr(default_factory=SitesStats)

    async def prepare(self) -> None:
        logger.info("Preparing CubeSites")
        if self.mapper:
            self.mapper.prepare()

    @property
    def n_sites(self) -> int:
        return sum(len(sites) for sites in self.sites.values())

    @property
    def n_cubes(self) -> int:
        return len(self.sites)

    def add_site(self, site: CubeSite) -> None:
        existing_sites = self.sites.get(site.cube_id, [])

        for existing_site in existing_sites:
            if existing_site.is_close(site):
                existing_site.start_time = min(
                    site.start_time,
                    existing_site.start_time,
                )
                existing_site.end_time = max(
                    site.end_time,
                    existing_site.end_time,
                )
                self.save()
                return

        if self.mapper:
            if station := self.mapper.get_station(site):
                site.set_station(station)
                self._stats.n_stations += 1
            else:
                logger.error("No station found for site %s", site)

        self.sites[site.cube_id].append(site)
        self._stats.n_sites += 1
        self.save()

    def get_site(self, datacube: CubeTraces) -> CubeSite | None:
        if sites := self.sites.get(datacube.cube_id, None):
            for site in sorted(sites, key=lambda s: s.start_time, reverse=True):
                if site.start_time <= datacube.start_time <= site.end_time:
                    return site
        logging.error(
            "No site found for %s, %s",
            datacube.cube_id,
            datacube.start_time,
        )
        self._stats.n_no_site += 1
        self.no_site_info.add(datacube.cube_id)
        return None

    def iter_sites(self) -> Iterator[CubeSite]:
        for sites in self.sites.values():
            for site in sites:
                yield site

    def iter_stations(self) -> Iterator[CubeSite]:
        sites = {}

        for site in self.iter_sites():
            if site.station is None:
                logger.warning(
                    "Skipping: No station for cube %s (%f, %f)",
                    site.cube_id,
                    site.lat,
                    site.lon,
                )
                continue

            if site.station_name not in sites:
                sites[site.station_name] = site
                continue

            existing_site = sites[site.station_name]
            if site.distance_to(existing_site) > 25.0:
                logger.warning(
                    "Site %s is more than 25 m away from existing site %s",
                    site,
                    existing_site,
                )
                continue
            existing_site.start_time = min(existing_site.start_time, site.start_time)
            existing_site.end_time = max(existing_site.end_time, site.end_time)

        yield from sites.values()

    def fill_endtimes(self, end_time: datetime = DATETIME_MAX) -> None:
        logger.info("Filling end times of sites")
        for sites in self.sites.values():
            sorted_sites = sorted(sites, key=lambda s: s.start_time)
            for i_site, site in enumerate(sorted_sites):
                if site.end_time:
                    continue
                if i_site + 1 < len(sorted_sites):
                    site.end_time = sorted_sites[i_site + 1].start_time
                else:
                    site.end_time = end_time

    async def post_process_datacube(
        self,
        cube: CubeTraces,
        site: CubeSite,
    ) -> CubeTraces:
        if site.station_name and site.station_name not in self.post_processors:
            return cube

        for processor in self.post_processors[site.station_name]:
            cube = await processor.process(cube)

        return cube

    async def process_datacubes(
        self,
        cubes: AsyncIterator[CubeTraces],
    ) -> AsyncIterator[CubeTraces]:
        logger.info("Processing datacubes")
        try:
            async for cube in cubes:
                new_site = await CubeSite.from_datacube_trace(cube)
                if new_site:
                    self.add_site(new_site)
                site = self.get_site(cube)
                if site:
                    cube.set_nsl(self.network, site.station_name, site.location)
                    if site.station:
                        if site.station_name in self.station_blacklist:
                            logger.info(
                                "Skipping blacklisted station %s", site.station_name
                            )
                            continue
                        for old, new in site.station.get_channel_map().items():
                            cube.rename_channels(old, new)
                        await self.post_process_datacube(cube, site)

                yield cube

            self.fill_endtimes()
        finally:
            self.save()

    def dump_csv(self, file: Path) -> None:
        logger.debug("Dumping CSV stations to %s", file)
        with file.open("w") as f:
            f.write(
                "cube_id,location,lat,lon,elevation,depth,start_time,station_name\n"
            )
            for site in self.iter_sites():
                f.write(
                    f"{site.cube_id},{site.location},{site.lat},{site.lon},"
                    f"{site.elevation},{site.depth},{site.start_time},"
                    f"{site.station_name}\n"
                )

    def dump_pyrocko_yaml(self, file: Path) -> None:
        logger.debug("Dumping Pyrocko stations to %s", file)
        stations = []
        for site in self.iter_sites():
            stations.append(site.as_pyrocko_station(self.network))
        dump_stations_yaml(stations, str(file))

    def save(self) -> None:
        if self._dump_path:
            self._dump_path.write_text(self.model_dump_json(indent=2))
            self.dump_csv(self._dump_path.with_suffix(".csv"))
            self.dump_pyrocko_yaml(self._dump_path.with_suffix(".yaml"))
        else:
            logger.warning("No dump path set, not saving.")

    async def fill_elevations(self) -> None:
        """Fill missing elevations by querying them from online service."""
        logger.info("Filling missing elevations")
        for site in self.iter_sites():
            if not site.has_valid_elevation():
                await site.query_elevation(self.elevation_model)
        self.save()

    @classmethod
    def load(cls, file: Path):
        sites = cls.model_validate_json(file.read_bytes())
        sites._dump_path = file
        logger.info(
            "loaded %d sites and %d cubes from %s",
            sites.n_sites,
            sites.n_cubes,
            file,
        )

        for site in sites.iter_sites():
            if site.station and sites.mapper:
                site.station.set_parent(sites.mapper)

        return sites
