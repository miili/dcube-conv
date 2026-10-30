from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, AsyncIterator, DefaultDict, Iterator

from pydantic import AliasChoices, BaseModel, Field, PositiveFloat, PrivateAttr
from pyrocko.io.datacube import coordinates_from_gps
from pyrocko.model import Station as PyrockoStation
from pyrocko.model import dump_stations_yaml

from dcube_conv.model import CubeFile, CubeId, Location
from dcube_conv.processors import ProcessorType
from dcube_conv.station_mapper import SensorID, Station, StationMapper
from dcube_conv.stats import Stats
from dcube_conv.utils import (
    DATETIME_MAX,
    ElevationModel,
    datetime_now,
    get_elevation,
)

if TYPE_CHECKING:
    from rich.table import Table

logger = logging.getLogger(__name__)

SAVE_INTERVAL = timedelta(seconds=60)


class StationNotFoundError(Exception):
    """No station of the layout is close to a cube location."""


class SitesStats(Stats):
    n_sites: int = 0
    n_stations: int = 0
    n_unmatched_cubes: int = 0

    def _populate_table(self, table: Table) -> None:
        table.add_row("Sites", str(self.n_sites))
        table.add_row("Sites not found", f"[red]{self.n_unmatched_cubes}")
        table.add_row("Stations", str(self.n_stations))
        table.add_row("Stations not found", f"[red]{self.n_sites - self.n_stations}")


class CubeLocation(Location):
    # Old state files use `location`.
    location_code: str = Field(
        default="", validation_alias=AliasChoices("location_code", "location")
    )
    cube_id: str
    sampling_rate: PositiveFloat

    start_time: datetime
    end_time: datetime

    station: Station | None = None

    @property
    def station_name(self) -> str:
        name = self.station.code if self.station else ""
        return re.sub(r"[^a-zA-Z0-9]", "", name)

    @property
    def wkt_geom(self) -> str:
        return f"POINT Z({self.lon} {self.lat} {self.elevation})"

    def set_station(self, station: Station) -> None:
        self.station = station

    @classmethod
    async def from_datacube_file(cls, datacube: CubeFile) -> CubeLocation | None:
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
            station=self.station_name or self.cube_id,
            location=self.location_code,
            lat=self.lat,
            lon=self.lon,
            elevation=self.elevation,
        )

    def has_valid_elevation(self) -> bool:
        return not (self.elevation == -999999.0 or self.elevation == 0.0)

    async def query_elevation(self, model: ElevationModel = "aster30m") -> None:
        self.elevation = await get_elevation(self.lat, self.lon, model)
        logger.info("New elevation for cube %s is %.1f m", self.cube_id, self.elevation)


class CubeLocations(BaseModel):
    network: str = Field(default="DC", max_length=2, pattern=r"[a-zA-Z0-9]")
    sites: DefaultDict[CubeId, list[CubeLocation]] = Field(
        default_factory=lambda: defaultdict(list)
    )
    elevation_model: ElevationModel = Field(default="aster30m")

    post_processors: dict[SensorID, list[ProcessorType]] = {}
    # Old configs use `station_blacklist`.
    exclude_stations: set[str] = Field(
        default_factory=set,
        validation_alias=AliasChoices("exclude_stations", "station_blacklist"),
    )

    mapper: StationMapper = Field(default_factory=StationMapper)
    no_site_info: set[CubeId] = Field(default_factory=set)

    _dump_path: Path | None = PrivateAttr(None)
    _last_save: datetime = PrivateAttr(default_factory=datetime_now)
    _stats: SitesStats = PrivateAttr(default_factory=SitesStats)

    async def prepare(self) -> None:
        logger.info("Preparing CubeSites")
        self.mapper.prepare()

    @property
    def n_sites(self) -> int:
        return sum(len(sites) for sites in self.sites.values())

    @property
    def n_cubes(self) -> int:
        return len(self.sites)

    def add_site(self, site: CubeLocation) -> None:
        existing_sites = self.sites.get(site.cube_id, [])
        self.write_site_csv(site, self._dump_path.with_suffix(".sites.csv"))

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
                # Only the time span grew, saved latest at the end of the run.
                self.save(throttle=True)
                return

        station = self.mapper.get_station(site)
        if not station:
            self.write_site_csv(site, self._dump_path.with_suffix(".bad_sites.csv"))
            raise StationNotFoundError(f"No station found for {site}")

        site.set_station(station)
        self._stats.n_stations += 1

        self.sites[site.cube_id].append(site)
        self._stats.n_sites += 1
        self.save()

    def get_site(self, datacube: CubeFile) -> CubeLocation | None:
        if sites := self.sites.get(datacube.cube_id, None):
            for site in sorted(sites, key=lambda s: s.start_time, reverse=True):
                if site.start_time <= datacube.start_time <= site.end_time:
                    return site
        logging.error(
            "No site found for %s, %s",
            datacube.cube_id,
            datacube.start_time,
        )
        self._stats.n_unmatched_cubes += 1
        self.no_site_info.add(datacube.cube_id)
        return None

    def iter_sites(self) -> Iterator[CubeLocation]:
        for sites in self.sites.values():
            for site in sites:
                yield site

    def iter_stations(self) -> Iterator[CubeLocation]:
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
        cube: CubeFile,
        site: CubeLocation,
    ) -> None:
        for processor in self.post_processors.get(site.station_name, []):
            await processor.process(cube)

    async def process_datacubes(
        self,
        cubes: AsyncIterator[CubeFile],
    ) -> AsyncIterator[CubeFile]:
        logger.info("Processing datacubes")
        try:
            async for cube in cubes:
                new_site = await CubeLocation.from_datacube_file(cube)
                if new_site:
                    try:
                        self.add_site(new_site)
                    except StationNotFoundError as e:
                        logger.error("Failed to add site: %s from %s", e, cube.path)
                        continue
                site = self.get_site(cube)
                if not site:
                    logger.debug(
                        "No site found for cube %s from %s",
                        cube.cube_id,
                        cube.path,
                    )
                    continue
                if not site.station:
                    logger.debug(
                        "No station name for cube %s from %s",
                        cube.cube_id,
                        cube.path,
                    )
                    continue

                cube.set_nsl(self.network, site.station_name, site.location_code)
                if site.station_name in self.exclude_stations:
                    logger.info("Skipping blacklisted station %s", site.station_name)
                    continue
                if site.station.sensor == "geophone" and cube.header.gain != 64:
                    logger.warning("wrong gain in %s, expected 64", cube.path)
                for old, new in site.station.get_channel_map().items():
                    cube.rename_channels(old, new)
                await self.post_process_datacube(cube, site)

                yield cube

            self.fill_endtimes()
        finally:
            self.save(views=True)

    def dump_csv(self, file: Path) -> None:
        logger.debug("Dumping CSV stations to %s", file)
        with file.open("w") as f:
            f.write(
                "cube_id,location,lat,lon,elevation,depth,start_time,station_name,WKT_geom\n"
            )
            for site in self.iter_sites():
                f.write(
                    f"{site.cube_id},{site.location_code},{site.lat},{site.lon},"
                    f"{site.elevation},{site.depth},{site.start_time},"
                    f"{site.station_name},{site.wkt_geom}\n"
                )

    def write_site_csv(self, site: CubeLocation, file: Path) -> None:
        if not file.exists():
            file.write_text(
                "cube_id,location,lat,lon,elevation,depth,start_time,station_name,WKT_geom\n"
            )
        with file.open("a") as f:
            f.write(
                f"{site.cube_id},{site.location_code},{site.lat},{site.lon},"
                f"{site.elevation},{site.depth},{site.start_time},"
                f"{site.station_name},{site.wkt_geom}\n"
            )

    def dump_pyrocko_yaml(self, file: Path) -> None:
        logger.debug("Dumping Pyrocko stations to %s", file)
        stations = []
        for site in self.iter_sites():
            stations.append(site.as_pyrocko_station(self.network))
        dump_stations_yaml(stations, str(file))

    def save(self, *, views: bool = False, throttle: bool = False) -> None:
        """Save the state atomically.

        Args:
            views (bool, optional): Also write the derived CSV and Pyrocko YAML
                files. Defaults to False.
            throttle (bool, optional): Skip saving if the last save is less
                than `SAVE_INTERVAL` ago. Defaults to False.
        """
        if not self._dump_path:
            logger.warning("No dump path set, not saving.")
            return
        now = datetime_now()
        if throttle and now - self._last_save < SAVE_INTERVAL:
            return

        tmp_file = self._dump_path.with_suffix(".tmp")
        tmp_file.write_text(self.model_dump_json(indent=2))
        tmp_file.replace(self._dump_path)
        self._last_save = now

        if views:
            self.dump_csv(self._dump_path.with_suffix(".csv"))
            self.dump_pyrocko_yaml(self._dump_path.with_suffix(".yaml"))

    async def fill_elevations(self) -> None:
        """Fill missing elevations by querying them from online service."""
        n_missing = len([s for s in self.iter_sites() if not s.has_valid_elevation()])
        logger.info(
            "Filling missing elevations for %d sites from %s",
            n_missing,
            self.elevation_model,
        )
        for site in self.iter_sites():
            if not site.has_valid_elevation():
                await site.query_elevation(self.elevation_model)
        self.save(views=True)

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
