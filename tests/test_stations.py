from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from pyrocko.trace import Trace

from dcube_conv.model import CubeFile
from dcube_conv.processors import FlipHorizontals
from dcube_conv.station_mapper import Station
from dcube_conv.stations import CubeLocation, CubeLocations, StationNotFoundError
from dcube_conv.utils import detect_datacube
from tests.test_utils import make_header

T0 = datetime(2026, 3, 10, tzinfo=timezone.utc)


def make_sites(tmp_path: Path, code: str = "EL-01") -> CubeLocations:
    sites = CubeLocations()
    sites._dump_path = tmp_path / "run.stations.json"
    station = Station(code=code, seismic_sensor="geophone", lat=50.0, lon=12.0)
    station.set_parent(sites.mapper)
    sites.mapper._features = [station]
    return sites


def make_location(lat: float = 50.0, lon: float = 12.0) -> CubeLocation:
    return CubeLocation(
        cube_id="ABC",
        lat=lat,
        lon=lon,
        sampling_rate=200.0,
        start_time=T0,
        end_time=T0,
    )


def make_cube() -> CubeFile:
    traces = [
        Trace(
            "",
            "ABC",
            "",
            channel,
            tmin=T0.timestamp(),
            deltat=0.005,
            ydata=np.arange(1, 11, dtype=np.int32),
        )
        for channel in ("p0", "p1", "p2")
    ]
    return CubeFile(
        path=Path("03100000.ABC"),
        header=detect_datacube(make_header()),
        traces=traces,
        gps_tags=None,
    )


async def collect(sites: CubeLocations, cube: CubeFile) -> list[CubeFile]:
    async def feed():
        yield cube

    return [c async for c in sites.process_datacubes(feed())]


def test_add_site_raises_station_not_found(tmp_path: Path) -> None:
    sites = make_sites(tmp_path)
    with pytest.raises(StationNotFoundError):
        sites.add_site(make_location(lat=51.0))
    assert sites.n_sites == 0


async def test_process_skips_cube_without_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def from_datacube_file(cube: CubeFile) -> CubeLocation:
        return make_location(lat=51.0)

    monkeypatch.setattr(CubeLocation, "from_datacube_file", from_datacube_file)
    sites = make_sites(tmp_path)
    assert await collect(sites, make_cube()) == []


async def test_process_raises_os_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def from_datacube_file(cube: CubeFile) -> CubeLocation:
        return make_location()

    def write_site_csv(self, *args) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(CubeLocation, "from_datacube_file", from_datacube_file)
    monkeypatch.setattr(CubeLocations, "write_site_csv", write_site_csv)
    sites = make_sites(tmp_path)
    with pytest.raises(OSError, match="disk full"):
        await collect(sites, make_cube())


async def test_post_processors_modify_cube_in_place(tmp_path: Path) -> None:
    sites = make_sites(tmp_path)
    sites.post_processors = {"EL01": [FlipHorizontals()]}
    site = make_location()
    sites.add_site(site)

    cube = make_cube()
    cube.set_nsl("DC", site.station_name, "")
    for old, new in site.station.get_channel_map().items():
        cube.rename_channels(old, new)

    await sites.post_process_datacube(cube, site)

    data = {tr.channel: tr.ydata for tr in cube.traces}
    np.testing.assert_array_equal(data["EPZ"], np.arange(1, 11))
    np.testing.assert_array_equal(data["EPN"], -np.arange(1, 11))
    np.testing.assert_array_equal(data["EPE"], -np.arange(1, 11))


def test_pyrocko_station_uses_stripped_code(tmp_path: Path) -> None:
    sites = make_sites(tmp_path, code="EL-01")
    site = make_location()
    sites.add_site(site)

    assert site.station_name == "EL01"
    assert site.as_pyrocko_station("DC").station == "EL01"


def test_load_old_field_names() -> None:
    old = {
        "station_blacklist": ["TA167"],
        "sites": {
            "ABC": [
                {
                    "cube_id": "ABC",
                    "location": "00",
                    "lat": 50.0,
                    "lon": 12.0,
                    "sampling_rate": 200.0,
                    "start_time": "2026-03-10T00:00:00Z",
                    "end_time": "2026-03-11T00:00:00Z",
                    "station": {
                        "name": "EL-01",
                        "location": "Sportfreunde Miesenheim",
                        "seismic_sensor": "geophone",
                        "lat": 50.0,
                        "lon": 12.0,
                    },
                }
            ]
        },
    }
    sites = CubeLocations.model_validate(old)
    site = sites.sites["ABC"][0]
    assert sites.exclude_stations == {"TA167"}
    assert site.location_code == "00"
    assert site.station.code == "EL-01"
    assert site.station.description == "Sportfreunde Miesenheim"

    dumped = sites.model_dump()
    location = dumped["sites"]["ABC"][0]
    assert "station_blacklist" not in dumped
    assert "location" not in location
    assert "name" not in location["station"]
    assert "location" not in location["station"]


def test_save_reduces_io(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from dcube_conv import stations

    now = datetime(2026, 3, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(stations, "datetime_now", lambda: now)
    sites = make_sites(tmp_path)
    state = sites._dump_path

    def saved_end_time() -> datetime:
        saved = json.loads(state.read_text())["sites"]["ABC"][0]["end_time"]
        return datetime.fromisoformat(saved)

    # A new location is saved right away, the derived views are not.
    sites.add_site(make_location())
    assert saved_end_time() == T0
    assert not state.with_suffix(".csv").exists()
    assert not state.with_suffix(".yaml").exists()

    # Merging a file only extends the time span and is throttled.
    later = make_location()
    later.end_time = datetime(2026, 3, 11, tzinfo=timezone.utc)
    sites.add_site(later)
    assert saved_end_time() == T0

    now += stations.SAVE_INTERVAL
    sites.add_site(later)
    assert saved_end_time() == later.end_time

    sites.save(views=True)
    assert state.with_suffix(".csv").exists()
    assert state.with_suffix(".yaml").exists()
    assert not state.with_suffix(".tmp").exists()
