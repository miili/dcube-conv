from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest
from obspy.core.inventory.response import Response
from pyrocko.io.datacube import DataCubeError, GPSTags
from pyrocko.trace import Trace

from dcube_conv import stations, stationxml
from dcube_conv.model import CubeFile, Location
from dcube_conv.processors import FlipHorizontals
from dcube_conv.station_mapper import Station, StationMapper
from dcube_conv.stations import Deployment, Deployments, locate_session
from dcube_conv.stationxml import StationXML
from dcube_conv.utils import detect_datacube
from tests.test_utils import make_header

SESSION_START = datetime(2026, 3, 9, 12, 43, 38, tzinfo=timezone.utc)
NSAMPLES = 1000


def gps_tags(lat: float | None, lon: float = 12.0) -> GPSTags:
    n = 10
    fix = np.zeros(n) if lat is None else np.ones(n)
    return GPSTags(
        position=np.arange(n),
        gps_time=np.arange(n, dtype=float),
        fix=fix,
        sat_count=np.full(n, 8),
        latitudes=np.full(n, lat or 0.0),
        longitudes=np.full(n, lon),
        elevations=np.full(n, 300.0),
        temperatures=np.zeros(n),
        gps_utc_offsets=np.zeros(n),
    )


def recordings(
    n_files: int = 3,
    device: str = "903",
    s_date: str = "26/03/09",
    gain: int = 64,
) -> list:
    return [
        (
            Path(f"{device}-{s_date.replace('/', '')}-{i}.{device}"),
            detect_datacube(
                make_header(s_date=s_date, dat_no=f"{i:03d}", device=device, gain=gain)
            ),
        )
        for i in range(n_files)
    ]


@pytest.fixture
def gps(monkeypatch: pytest.MonkeyPatch) -> dict[str, GPSTags]:
    """GPS tags per file name, files not listed have no fix."""
    tags: dict[str, GPSTags] = {}

    def get_time_infos(file: str) -> tuple:
        if file.startswith("broken"):
            raise DataCubeError("broken")
        return {}, tags.get(file, gps_tags(None)), NSAMPLES

    monkeypatch.setattr(stations, "get_time_infos", get_time_infos)
    return tags


@pytest.fixture
def layout(monkeypatch: pytest.MonkeyPatch) -> list[Station]:
    """Station layout replacing the GeoPackage."""
    features = [
        Station(code="EL-01", seismic_sensor="geophone", lat=50.0, lon=12.0),
        Station(code="EL-02", seismic_sensor="geophone", lat=50.1, lon=12.0),
    ]

    def prepare(self: StationMapper) -> None:
        for station in features:
            station.set_parent(self)
        self._features = features

    monkeypatch.setattr(StationMapper, "prepare", prepare)
    return features


def make_cube(header_data: bytes, location: Location | None = None) -> CubeFile:
    header = detect_datacube(header_data)
    traces = [
        Trace(
            "",
            header.device,
            "",
            channel,
            tmin=header.start_time.timestamp(),
            deltat=1.0 / header.sampling_rate,
            ydata=np.arange(1, 11, dtype=np.int32),
        )
        for channel in ("p0", "p1", "p2")
    ]
    return CubeFile(
        path=Path(f"03100000.{header.device}"),
        header=header,
        traces=traces,
        gps_tags=None,
        location=location,
    )


async def collect(deployments: Deployments, *cubes: CubeFile) -> list[CubeFile]:
    async def feed():
        for cube in cubes:
            yield cube

    return [c async for c in deployments.process_datacubes(feed())]


def locate_all(gps: dict[str, GPSTags], lat: float, *sessions: list) -> None:
    for session in sessions:
        for file, _ in session:
            gps[str(file)] = gps_tags(lat)


def test_locate_session(gps: dict[str, GPSTags]) -> None:
    session = recordings(n_files=3)
    locate_all(gps, 50.0, session)

    deployment = locate_session(session)
    assert deployment is not None
    assert (deployment.lat, deployment.lon, deployment.elevation) == (50.0, 12.0, 300.0)
    assert deployment.cube_id == "903"
    assert deployment.session_start == SESSION_START
    assert deployment.start_time == SESSION_START
    last = session[-1][1]
    assert deployment.end_time == last.start_time + timedelta(
        seconds=(NSAMPLES - 1) / 200
    )
    assert (deployment.n_files, deployment.gain, deployment.sampling_rate) == (
        3,
        64,
        200,
    )


def test_locate_session_end_from_gps(gps: dict[str, GPSTags]) -> None:
    session = recordings(n_files=3)
    locate_all(gps, 50.0, session)
    last = session[-1][1]
    # The cube clock runs a minute ahead, GPS time is 18 s ahead of UTC.
    true_start = last.start_time - timedelta(minutes=1)
    tags = gps[str(session[-1][0])]
    gps[str(session[-1][0])] = tags._replace(
        gps_time=true_start.timestamp() + 18.0 + tags.position / 200
    )

    deployment = locate_session(session)
    assert deployment is not None
    assert deployment.end_time == true_start + timedelta(seconds=(NSAMPLES - 1) / 200)


def test_locate_session_uses_earlier_fix(gps: dict[str, GPSTags]) -> None:
    session = recordings(n_files=3)
    gps[str(session[0][0])] = gps_tags(50.0)

    deployment = locate_session(session)
    assert deployment is not None
    assert deployment.lat == 50.0
    # The end still comes from the last file.
    assert deployment.end_time > session[-1][1].start_time


def test_locate_session_without_fix(gps: dict[str, GPSTags]) -> None:
    session = [(Path("broken.903"), recordings(1)[0][1]), *recordings(2)[1:]]
    assert locate_session(session) is None


async def test_prepare_matches_sessions_to_stations(
    gps: dict[str, GPSTags], layout: list[Station]
) -> None:
    near = recordings(device="903")
    far = recordings(device="904")
    no_gps = recordings(device="905")
    locate_all(gps, 50.1, near)
    locate_all(gps, 51.0, far)

    deployments = Deployments()
    await deployments.prepare({s[0][1].session_id: s for s in (near, far, no_gps)})
    located = {d.cube_id: d for d in deployments.iter_deployments()}
    assert located["903"].station_code == "EL02"
    assert located["904"].station is None
    assert "905" not in located

    stats = deployments._stats
    assert (stats.n_sessions, stats.n_no_gps, stats.n_no_station) == (3, 1, 1)
    assert stats.n_stations == 1


async def test_prepare_reuses_located_sessions(
    gps: dict[str, GPSTags], layout: list[Station], tmp_path: Path
) -> None:
    session = recordings()
    locate_all(gps, 50.0, session)
    sessions = {session[0][1].session_id: session}

    deployments = Deployments()
    deployments.load_state(tmp_path / "run.deployments.json")
    await deployments.prepare(sessions)

    gps.clear()  # Locating again would fail
    reloaded = Deployments()
    reloaded.load_state(tmp_path / "run.deployments.json")
    await reloaded.prepare(sessions)
    assert reloaded._stats.n_cached == 1
    assert next(reloaded.iter_deployments()).station_code == "EL01"


async def test_prepare_relocates_old_versions(
    gps: dict[str, GPSTags], layout: list[Station], tmp_path: Path
) -> None:
    session = recordings()
    locate_all(gps, 50.0, session)
    sessions = {session[0][1].session_id: session}

    deployments = Deployments()
    deployments.load_state(tmp_path / "run.deployments.json")
    await deployments.prepare(sessions)
    old = next(deployments.iter_deployments())
    old.locate_version = 1
    old.elevation = 555.0  # Filled online
    deployments.save()

    for file, _ in session:  # The GPS gives no elevation
        gps[str(file)] = gps[str(file)]._replace(elevations=np.zeros(10))
    reloaded = Deployments()
    reloaded.load_state(tmp_path / "run.deployments.json")
    await reloaded.prepare(sessions)
    assert reloaded._stats.n_cached == 0
    relocated = next(reloaded.iter_deployments())
    assert relocated.locate_version == stations.LOCATE_VERSION
    assert relocated.elevation == 555.0


async def test_process_assigns_files_by_session(
    gps: dict[str, GPSTags], layout: list[Station]
) -> None:
    session = recordings()
    locate_all(gps, 50.0, session)
    deployments = Deployments(network="X2")
    await deployments.prepare({session[0][1].session_id: session})

    # A file without GPS of a located session, and a file of an unknown session.
    cube = make_cube(make_header(dat_no="001"))
    unknown = make_cube(make_header(device="999"))
    assigned = await collect(deployments, cube, unknown)

    assert assigned == [cube]
    assert cube.deployment is next(deployments.iter_deployments())
    assert {tr.nslc_id for tr in cube.traces} == {
        ("X2", "EL01", "", "EPZ"),
        ("X2", "EL01", "", "EPN"),
        ("X2", "EL01", "", "EPE"),
    }
    stats = deployments._stats
    assert (stats.n_files_assigned, stats.n_files_no_station) == (1, 1)


async def test_process_flags_files_off_site(
    gps: dict[str, GPSTags], layout: list[Station]
) -> None:
    session = recordings()
    locate_all(gps, 50.0, session)
    deployments = Deployments()
    await deployments.prepare({session[0][1].session_id: session})

    cube = make_cube(make_header(), location=Location(lat=50.01, lon=12.0))
    assert await collect(deployments, cube) == [cube]
    assert deployments._stats.n_files_off_site == 1


async def test_process_excludes_stations_and_runs_processors(
    gps: dict[str, GPSTags], layout: list[Station]
) -> None:
    near = recordings(device="903")
    other = recordings(device="904")
    locate_all(gps, 50.0, near)
    locate_all(gps, 50.1, other)
    deployments = Deployments(
        exclude_stations={"EL02"},
        post_processors={"EL01": [FlipHorizontals()]},
    )
    await deployments.prepare({s[0][1].session_id: s for s in (near, other)})

    cube = make_cube(make_header(device="903"))
    excluded = make_cube(make_header(device="904"))
    assert await collect(deployments, cube, excluded) == [cube]
    assert deployments._stats.n_files_excluded == 1

    data = {tr.channel: tr.ydata for tr in cube.traces}
    np.testing.assert_array_equal(data["EPZ"], np.arange(1, 11))
    np.testing.assert_array_equal(data["EPN"], -np.arange(1, 11))


async def test_iter_stations_groups_deployments(
    gps: dict[str, GPSTags], layout: list[Station]
) -> None:
    # Cube 903 is at EL-01, cube 904 takes over, then 903 comes back.
    first = recordings(device="903", s_date="26/03/01")
    swap = recordings(device="904", s_date="26/04/01")
    back = recordings(device="903", s_date="26/05/01")
    locate_all(gps, 50.0, first, swap, back)
    deployments = Deployments()
    await deployments.prepare({s[0][1].session_id: s for s in (first, swap, back)})

    (station,) = deployments.iter_stations()
    assert station.code == "EL01"
    assert [d.cube_id for d in station.deployments] == ["903", "904", "903"]
    assert station.start_time == first[0][1].start_time
    # Deployments keep their own time spans.
    assert station.deployments[0].end_time < station.deployments[1].start_time


def test_load_state_ignores_old_state(tmp_path: Path) -> None:
    state = tmp_path / "run.deployments.json"
    state.write_text(json.dumps({"sites": {}}))
    deployments = Deployments()
    deployments.load_state(state)
    assert list(deployments.iter_deployments()) == []


def test_load_old_config_field_names() -> None:
    deployments = Deployments.model_validate({"station_blacklist": ["TA167"]})
    assert deployments.exclude_stations == {"TA167"}


async def test_views_use_stripped_codes(
    gps: dict[str, GPSTags], layout: list[Station], tmp_path: Path
) -> None:
    session = recordings()
    locate_all(gps, 50.0, session)
    deployments = Deployments()
    deployments.load_state(tmp_path / "run.deployments.json")
    await deployments.prepare({session[0][1].session_id: session})
    deployments.save(views=True)

    assert "station: EL01" in (tmp_path / "run.deployments.yaml").read_text()
    assert ",EL01," in (tmp_path / "run.deployments.csv").read_text()
    assert not (tmp_path / "run.deployments.tmp").exists()


def make_deployment(
    cube_id: str,
    start_day: float,
    end_day: float,
    gain: int = 64,
    lat: float = 50.0,
) -> Deployment:
    return Deployment(
        cube_id=cube_id,
        session_start=SESSION_START + timedelta(days=start_day),
        start_time=SESSION_START + timedelta(days=start_day),
        end_time=SESSION_START + timedelta(days=end_day),
        sampling_rate=200,
        gain=gain,
        n_files=1,
        lat=lat,
        lon=12.0,
        elevation=300.0,
    )


def test_channel_epochs_merge_restarts_and_swaps() -> None:
    # Restart of 903, then 904 takes over about 6 m away.
    epochs = stationxml.channel_epochs(
        [
            make_deployment("903", 0, 10),
            make_deployment("903", 10.1, 20),
            make_deployment("904", 20.1, 30, lat=50.0001),
        ],
        max_distance=15.0,
    )
    (epoch,) = epochs
    assert epoch.n_deployments == 3
    assert epoch.lat == pytest.approx(50.0000333)
    assert epoch.start_time == SESSION_START
    assert epoch.end_time == SESSION_START + timedelta(days=30)


def test_channel_epochs_split_on_gain_and_move() -> None:
    epochs = stationxml.channel_epochs(
        [
            make_deployment("903", 0, 10),
            make_deployment("904", 10.1, 20, gain=8),
            make_deployment("905", 20.1, 30, gain=8, lat=50.001),
        ],
        max_distance=15.0,
    )
    assert [(e.gain, e.lat) for e in epochs] == [(64, 50.0), (8, 50.0), (8, 50.001)]


def test_channel_epochs_do_not_overlap() -> None:
    # 904 is switched on before 903 is stopped.
    epochs = stationxml.channel_epochs(
        [make_deployment("904", 9.9, 20, gain=8), make_deployment("903", 0, 10)],
        max_distance=15.0,
    )
    assert [e.gain for e in epochs] == [64, 8]
    assert epochs[0].end_time == epochs[1].start_time
    assert epochs[1].end_time == SESSION_START + timedelta(days=20)


async def test_stationxml_has_epoch_per_response(
    gps: dict[str, GPSTags],
    layout: list[Station],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = []

    def get_response(root: Path, sensor: tuple, datalogger: tuple) -> Response:
        keys.append(datalogger)
        return Response.from_paz(
            zeros=[], poles=[], stage_gain=1.0, input_units="M/S", output_units="COUNTS"
        )

    monkeypatch.setattr(stationxml, "get_response", get_response)
    first = recordings(device="903", s_date="26/03/01", gain=64)
    swap = recordings(device="904", s_date="26/04/01", gain=64)
    regain = recordings(device="905", s_date="26/05/01", gain=8)
    locate_all(gps, 50.0, first, regain)
    locate_all(gps, 50.0001, swap)
    deployments = Deployments(network="X2")
    await deployments.prepare({s[0][1].session_id: s for s in (first, swap, regain)})

    inventory = StationXML().get_inventory(deployments)
    (station,) = inventory.networks[0].stations
    assert station.code == "EL01"
    assert len(station.channels) == 6
    assert sorted({round(c.latitude, 6) for c in station.channels}) == [
        50.0,
        50.00005,
    ]
    assert {c.data_logger.serial_number for c in station.channels} == {None}
    assert keys == [
        ("DiGOSOmnirecs", "DataCube", "64", "200 Hz"),
        ("DiGOSOmnirecs", "DataCube", "8", "200 Hz"),
    ]


def test_stationxml_accepts_nrl_v1_datalogger() -> None:
    response = stationxml.StationResponse.model_validate(
        {
            "sensor_name": "geophone",
            "sensor": ["SensorNederland", "PE-6B", "4.5 s", "375", "None"],
            "datalogger": ["DiGOS/Omnirecs", "DATACUBE", "64"],
            "sensor_description": "4.5 Hz geophone",
        }
    )
    assert response.datalogger == ("DiGOSOmnirecs", "DataCube")


def test_deployment_extends_to_file_times() -> None:
    deployment = Deployment(
        cube_id="903",
        session_start=SESSION_START,
        start_time=SESSION_START,
        end_time=SESSION_START + timedelta(days=1),
        sampling_rate=200,
        gain=64,
        n_files=1,
        lat=50.0,
        lon=12.0,
    )
    cube = make_cube(make_header(dat_no="000"))
    for trace in cube.traces:
        trace.shift(-5.0)
    deployment.extend(cube)
    assert deployment.start_time == SESSION_START - timedelta(seconds=5)
