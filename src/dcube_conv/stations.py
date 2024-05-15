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
from dcube_conv.station_mapper import Station, StationMapper
from dcube_conv.stats import Stats
from dcube_conv.utils import ElevationModel, get_elevation

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
    channel_map: dict[str, str] = Field(default_factory=dict)

    start_time: datetime
    end_time: datetime | None = None

    station: Station | None = None

    @property
    def station_name(self) -> str:
        return self.station.name if self.station else ""

    def set_station(self, station: Station) -> None:
        self.station = station
        self.channel_map = station.get_channel_map()

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

    mapper: StationMapper | None = Field(default_factory=StationMapper)
    no_site_info: set[CubeId] = Field(default_factory=set)

    _dump_path: Path | None = PrivateAttr(None)
    _stats: SitesStats = PrivateAttr(default_factory=SitesStats)

    async def prepare(self) -> None:
        if self.mapper:
            self.mapper.prepare()

    def add_site(self, site: CubeSite) -> None:
        existing_sites = self.sites.get(site.cube_id, [])

        for existing_site in existing_sites:
            if existing_site.is_close(site):
                if existing_site.start_time > site.start_time:
                    existing_site.start_time = site.start_time
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
                if site.start_time <= datacube.start_time:
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

    def fill_endtimes(self, end_time: datetime = datetime.max) -> None:
        for sites in self.sites.values():
            for i_site, site in enumerate(sorted(sites, key=lambda s: s.start_time)):
                if i_site + 1 < len(sites):
                    site.end_time = sites[i_site + 1].start_time
                else:
                    site.end_time = end_time

    async def process_datacubes(
        self,
        cubes: AsyncIterator[CubeTraces],
    ) -> AsyncIterator[CubeTraces]:
        async for cube in cubes:
            new_site = await CubeSite.from_datacube_trace(cube)
            if new_site:
                self.add_site(new_site)
            site = self.get_site(cube)
            if site:
                cube.set_nsl(self.network, site.station_name, site.location)
                for old, new in site.channel_map.items():
                    cube.rename_channels(old, new)

            yield cube

        self.fill_endtimes()
        self.save()
        # TODO Fill in missing elevations!

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
