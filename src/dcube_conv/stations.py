from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any, AsyncIterator, ClassVar, Iterator, NamedTuple, Self

from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    PositiveFloat,
    PrivateAttr,
    TypeAdapter,
    ValidationError,
)
from pyrocko.io.datacube import DataCubeError, get_time_infos
from pyrocko.model import Station as PyrockoStation
from pyrocko.model import dump_stations_yaml
from rich.console import RenderableType
from rich.progress import Progress

from dcube_conv.model import CubeFile, Location
from dcube_conv.processors import ProcessorType
from dcube_conv.station_mapper import Station, StationMapper
from dcube_conv.stats import Stats, StatsTable, make_progress, problem
from dcube_conv.utils import (
    DataCubeHeader,
    ElevationModel,
    SessionId,
    get_elevation,
    gps_end_time,
)

logger = logging.getLogger(__name__)

# Files read for a GPS fix when locating a session, starting with the last one.
MAX_LOCATE_ATTEMPTS = 8
# Largest offset of the cube clock from GPS time at the end of a session.
MAX_CLOCK_DRIFT = timedelta(hours=6)
# Sessions located by an older version are located again.
LOCATE_VERSION = 2

Recordings = list[tuple[Path, DataCubeHeader]]


class DeploymentStats(Stats):
    title: ClassVar[str] = "Deployments"

    n_sessions: int = 0
    i_sessions: int = 0
    n_cached: int = 0
    n_no_gps: int = 0
    n_no_station: int = 0
    n_stations: int = 0
    n_layout: int = 0

    n_files_assigned: int = 0
    n_files_no_station: int = 0
    n_files_excluded: int = 0
    n_files_off_site: int = 0

    _bar: Progress = PrivateAttr(default_factory=make_progress)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._position = 1
        self._task = self._bar.add_task("Locating", total=None, detail="")

    def progress_bar(self) -> RenderableType | None:
        if not self.n_sessions:
            return None
        self._bar.update(
            self._task,
            total=self.n_sessions,
            completed=self.i_sessions,
            detail=f"{self.i_sessions:,}/{self.n_sessions:,} sessions",
        )
        return self._bar

    def _populate(self, table: StatsTable) -> None:
        table.row(
            "sessions",
            self.n_sessions,
            detail=f"{self.n_cached:,} from state"
            f" · {self.i_sessions - self.n_cached:,} located from GPS"
            f" · {problem(self.n_no_gps, 'no GPS fix')}",
        )
        table.row(
            "stations",
            self.n_stations,
            detail=f"of {self.n_layout:,} in layout"
            f" · {problem(self.n_no_station, 'sessions without station')}",
        )
        table.row(
            "files",
            self.n_files_assigned,
            "bold green",
            detail=f"assigned · {problem(self.n_files_no_station, 'without station')}"
            f" · {self.n_files_excluded:,} excluded"
            f" · {problem(self.n_files_off_site, 'off site')}",
        )


class Deployment(Location):
    """A recording session of a cube, the cube stays at one place."""

    cube_id: str
    session_start: datetime
    start_time: datetime
    end_time: datetime
    sampling_rate: PositiveFloat
    gain: int
    n_files: int
    station: Station | None = None
    location_code: str = ""
    locate_version: int = 1
    """Version 1 took the session end from the drifting cube clock."""

    @property
    def session_id(self) -> SessionId:
        return self.cube_id, self.session_start

    @property
    def station_code(self) -> str:
        """Station code reduced to the characters allowed in miniSEED."""
        code = self.station.code if self.station else ""
        return re.sub(r"[^a-zA-Z0-9]", "", code)

    @property
    def wkt_geom(self) -> str:
        return f"POINT Z({self.lon} {self.lat} {self.elevation})"

    def extend(self, cube: CubeFile) -> None:
        """Extend the time span to the GPS corrected times of a file."""
        self.start_time = min(self.start_time, cube.start_time)
        self.end_time = max(self.end_time, cube.end_time)

    def has_valid_elevation(self) -> bool:
        return self.elevation not in (-999999.0, 0.0)

    async def query_elevation(self, model: ElevationModel = "aster30m") -> None:
        self.elevation = await get_elevation(self.lat, self.lon, model)
        logger.info("New elevation for cube %s is %.1f m", self.cube_id, self.elevation)


DEPLOYMENTS = TypeAdapter(list[Deployment])


def locate_session(recordings: Recordings) -> Deployment | None:
    """Locate a recording session from the GPS tags of its files.

    The last file is read first, it also gives the end of the session.

    Args:
        recordings (Recordings): The files of the session, sorted by file number.

    Returns:
        Deployment | None: The deployment, None without a GPS fix.
    """
    first = recordings[0][1]
    last = recordings[-1][1]
    end_time = last.start_time + timedelta(days=1)

    step = max(1, len(recordings) // MAX_LOCATE_ATTEMPTS)
    candidates = recordings[::-1][::step][:MAX_LOCATE_ATTEMPTS]
    location = None
    for file, header in candidates:
        try:
            _, gps_tags, nsamples = get_time_infos(str(file))
        except (DataCubeError, OSError) as exc:
            logger.debug("Cannot read GPS of %s: %s", file, exc)
            continue
        if header is last:
            end_time = header.start_time + timedelta(
                seconds=(nsamples - 1) / header.sampling_rate
            )
            gps_end = gps_end_time(gps_tags, nsamples, header.sampling_rate)
            if gps_end and abs(gps_end - end_time) < MAX_CLOCK_DRIFT:
                end_time = gps_end
            else:
                logger.warning(
                    "No GPS time for the end of session %s of cube %s,"
                    " using the cube clock",
                    header.session_start,
                    header.device,
                )
        if location := Location.from_gps_tags(gps_tags):
            break
    if location is None:
        return None

    for setting in ("sampling_rate", "gain"):
        values = {getattr(header, setting) for _, header in recordings}
        if len(values) > 1:
            logger.warning(
                "Session %s of cube %s changes %s: %s",
                first.session_start,
                first.device,
                setting,
                sorted(values),
            )
    return Deployment(
        cube_id=first.device,
        session_start=first.session_start,
        start_time=first.start_time,
        end_time=end_time,
        sampling_rate=first.sampling_rate,
        gain=first.gain,
        n_files=len(recordings),
        locate_version=LOCATE_VERSION,
        **location.model_dump(),
    )


class StationDeployments(NamedTuple):
    """A station of the layout and the cube deployments recording it."""

    station: Station
    deployments: list[Deployment]

    @property
    def code(self) -> str:
        return self.deployments[0].station_code

    @property
    def start_time(self) -> datetime:
        return min(d.start_time for d in self.deployments)

    @property
    def end_time(self) -> datetime:
        return max(d.end_time for d in self.deployments)


class Deployments(BaseModel):
    """Assigns datacube files to stations through their recording sessions.

    A cube does not move within a recording session, so each session is one
    deployment. It is located once from GPS and matched to the nearest station
    of the layout.
    """

    network: str = Field(default="DC", max_length=2, pattern=r"^[a-zA-Z0-9]+$")
    elevation_model: ElevationModel = Field(default="aster30m")

    post_processors: dict[str, list[ProcessorType]] = Field(
        default_factory=dict,
        description="Processors per station code.",
    )
    # Old configs use `station_blacklist`.
    exclude_stations: set[str] = Field(
        default_factory=set,
        validation_alias=AliasChoices("exclude_stations", "station_blacklist"),
    )

    mapper: StationMapper = Field(default_factory=StationMapper)

    _deployments: dict[SessionId, Deployment] = PrivateAttr(default_factory=dict)
    _state_file: Path | None = PrivateAttr(None)
    _stats: DeploymentStats = PrivateAttr(default_factory=DeploymentStats)

    async def prepare(self, sessions: dict[SessionId, Recordings]) -> None:
        """Locate new recording sessions and match all to stations.

        Args:
            sessions (dict[SessionId, Recordings]): Files per recording session.
        """
        stats = self._stats
        self.mapper.prepare()
        stats.n_sessions = len(sessions)
        stats.n_layout = len(self.mapper._features)

        async def locate(recordings: Recordings) -> None:
            deployment = await asyncio.to_thread(locate_session, recordings)
            stats.i_sessions += 1
            if deployment is None:
                stats.n_no_gps += 1
                header = recordings[0][1]
                logger.warning(
                    "No GPS fix in session %s of cube %s, skipping %d files",
                    header.session_start,
                    header.device,
                    len(recordings),
                )
                return
            # Keep an elevation filled online when the GPS gives none.
            known = self._deployments.get(deployment.session_id)
            if (
                known
                and known.has_valid_elevation()
                and not deployment.has_valid_elevation()
            ):
                deployment.elevation = known.elevation
            self._deployments[deployment.session_id] = deployment

        new_sessions = []
        for session_id, recordings in sessions.items():
            known = self._deployments.get(session_id)
            if (
                known
                and known.n_files == len(recordings)
                and known.locate_version == LOCATE_VERSION
            ):
                stats.n_cached += 1
                stats.i_sessions += 1
            else:
                new_sessions.append(recordings)
        await asyncio.gather(*(locate(recordings) for recordings in new_sessions))

        # Match all, the layout may have changed since the last run.
        for deployment in self._deployments.values():
            deployment.station = self.mapper.get_station(deployment)
            if deployment.station is None:
                stats.n_no_station += 1
                logger.warning(
                    "No station within %.0f m of cube %s at %.5f, %.5f (%s)",
                    self.mapper.distance_threshold_meters,
                    deployment.cube_id,
                    deployment.lat,
                    deployment.lon,
                    deployment.session_start,
                )
        stats.n_stations = len(
            {d.station_code for d in self._deployments.values() if d.station}
        )
        self.save()

    def get(self, cube: CubeFile) -> Deployment | None:
        return self._deployments.get(cube.header.session_id)

    def iter_deployments(self) -> Iterator[Deployment]:
        yield from sorted(
            self._deployments.values(), key=lambda d: (d.cube_id, d.start_time)
        )

    def iter_stations(self) -> Iterator[StationDeployments]:
        """Stations of the layout with their deployments, sorted by time."""
        by_code: defaultdict[str, list[Deployment]] = defaultdict(list)
        for deployment in sorted(
            self._deployments.values(), key=lambda d: d.start_time
        ):
            if deployment.station:
                by_code[deployment.station_code].append(deployment)

        for code, deployments in sorted(by_code.items()):
            for previous, deployment in pairwise(deployments):
                if deployment.start_time < previous.end_time:
                    logger.warning(
                        "Overlapping deployments at %s: cube %s until %s,"
                        " cube %s from %s",
                        code,
                        previous.cube_id,
                        previous.end_time,
                        deployment.cube_id,
                        deployment.start_time,
                    )
            yield StationDeployments(deployments[-1].station, deployments)

    async def process_datacubes(
        self,
        cubes: AsyncIterator[CubeFile],
    ) -> AsyncIterator[CubeFile]:
        stats = self._stats
        try:
            async for cube in cubes:
                deployment = self.get(cube)
                if deployment is None or deployment.station is None:
                    stats.n_files_no_station += 1
                    logger.debug("No station for %s", cube.path)
                    continue
                code = deployment.station_code
                if code in self.exclude_stations:
                    stats.n_files_excluded += 1
                    continue
                if cube.location:
                    distance = cube.location.surface_distance_to(deployment)
                    if distance > self.mapper.distance_threshold_meters:
                        stats.n_files_off_site += 1
                        logger.warning(
                            "%s is %.0f m away from its deployment at %s",
                            cube.path,
                            distance,
                            code,
                        )

                cube.deployment = deployment
                deployment.extend(cube)
                cube.set_nsl(self.network, code, deployment.location_code)
                for old, new in deployment.station.get_channel_map().items():
                    cube.rename_channels(old, new)
                for processor in self.post_processors.get(code, []):
                    await processor.process(cube)

                stats.n_files_assigned += 1
                yield cube
        finally:
            self.save(views=True)

    def dump_csv(self, file: Path) -> None:
        logger.debug("Dumping CSV deployments to %s", file)
        with file.open("w") as f:
            f.write(
                "cube_id,session_start,start_time,end_time,station,location,"
                "lat,lon,elevation,sampling_rate,gain,n_files,WKT_geom\n"
            )
            for d in self.iter_deployments():
                f.write(
                    f"{d.cube_id},{d.session_start},{d.start_time},{d.end_time},"
                    f"{d.station_code},{d.location_code},{d.lat},{d.lon},"
                    f"{d.elevation},{d.sampling_rate},{d.gain},{d.n_files},"
                    f"{d.wkt_geom}\n"
                )

    def dump_pyrocko_yaml(self, file: Path) -> None:
        logger.debug("Dumping Pyrocko stations to %s", file)
        stations = []
        for station in self.iter_stations():
            latest = station.deployments[-1]
            stations.append(
                PyrockoStation(
                    network=self.network,
                    station=station.code,
                    location=latest.location_code,
                    lat=latest.lat,
                    lon=latest.lon,
                    elevation=latest.elevation,
                )
            )
        dump_stations_yaml(stations, str(file))

    def save(self, *, views: bool = False) -> None:
        """Save the deployments atomically.

        Args:
            views (bool, optional): Also write the derived CSV and Pyrocko YAML
                files. Defaults to False.
        """
        if not self._state_file:
            return
        tmp_file = self._state_file.with_suffix(".tmp")
        tmp_file.write_bytes(
            DEPLOYMENTS.dump_json(list(self.iter_deployments()), indent=2)
        )
        tmp_file.replace(self._state_file)
        if views:
            self.dump_csv(self._state_file.with_suffix(".csv"))
            self.dump_pyrocko_yaml(self._state_file.with_suffix(".yaml"))

    def load_state(self, file: Path) -> None:
        """Load located deployments of earlier runs and save to this file."""
        self._state_file = file
        if not file.exists():
            return
        try:
            deployments = DEPLOYMENTS.validate_json(file.read_bytes())
        except ValidationError as exc:
            logger.warning("Ignoring incompatible state %s: %s", file, exc)
            return
        for deployment in deployments:
            if deployment.station:
                deployment.station.set_parent(self.mapper)
            self._deployments[deployment.session_id] = deployment
        logger.info("Loaded %d deployments from %s", len(deployments), file)

    @classmethod
    def from_run_config(cls, file: Path) -> Self:
        """Load the station configuration and deployments of a conversion run.

        Args:
            file (Path): The conversion config, e.g. run.json.
        """
        config = json.loads(file.read_text())
        deployments = cls.model_validate(config.get("stations", {}))
        deployments.load_state(state_file(file))
        return deployments

    async def fill_elevations(self) -> None:
        """Fill missing elevations by querying them from online service."""
        missing = [d for d in self.iter_deployments() if not d.has_valid_elevation()]
        logger.info(
            "Filling missing elevations for %d deployments from %s",
            len(missing),
            self.elevation_model,
        )
        for deployment in missing:
            await deployment.query_elevation(self.elevation_model)
        self.save(views=True)


def state_file(config_file: Path) -> Path:
    """State file of a conversion run, next to its config."""
    return config_file.with_name(f"{config_file.stem}.deployments.json")
