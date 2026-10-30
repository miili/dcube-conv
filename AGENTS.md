# AGENTS.md

`dcube-conv` converts raw DiGOS/Omnirecs **DATA-CUBE³** recordings from large-N
seismic deployments (e.g. ELISE, Eger rift) into an SDS miniSEED archive. It
also builds a StationXML inventory for the stations. Each cube's GPS position
is matched to a planned station layout from a GeoPackage, which sets the
station names and channel codes.

## Environment

- Python >= 3.11. The dev environment is the conda env **`dconv`**, which has
  an editable install. `base` does not have the package installed.
  ```sh
  conda activate dconv          # or call /home/isken/miniconda3/envs/dconv/bin/python
  python -m pytest -q
  ruff check src tests && ruff format src tests
  pre-commit run --all-files
  ```
- Dependencies are declared in `pyproject.toml`, and `pdm.lock` exists, but
  `pdm` is not installed on this machine. Install with `pip install -e .`.
- Ruff rules: `W N DTZ RET SIM B C4 I RUF`, plus the defaults. Use
  timezone-aware datetimes (`DTZ`). Existing naive `datetime.now()` calls are
  marked with `# noqa DTZ005`.
- `matplotlib` is used by `plot.py` but is **not** declared as a dependency.

## CLI (`dcube`, entry point `dcube_conv.app.dcube_convert:main`)

| Command | Purpose |
|---|---|
| `dcube init > run.json` | Print the default `Converter` config as JSON |
| `dcube convert run.json` | Run the conversion and write `run.log` |
| `dcube stationxml init > sx.json` | Print the default `StationXML` config |
| `dcube stationxml create [--fill-elevations] sx.json run.stations.json` | Write `sx.xml` and `sx.missing.csv` |
| `dcube plot run.stations.json` | Plot site coverage (exploratory) |

`-v` sets the log level to DEBUG and turns on asyncio debug mode.

### Files written next to the config (`run.json`)

- `run.stations.json`: the serialized `CubeSites` state, which holds the
  discovered cube sites and their mapped stations. It is reloaded on the next
  run when present. It is written atomically when a new location is added and
  at most every 60 s otherwise. The derived `.csv` and `.yaml` (Pyrocko
  stations) files are written only at the end of the run. Every datacube
  file appends a row to `.sites.csv` or `.bad_sites.csv`.
- `run.progress`: one converted file path per line. Files listed here are
  skipped on the next run, which makes conversion **resumable**. Delete it to
  re-convert.
- `run.log`: the log file.

## Architecture (`src/dcube_conv/`)

The pipeline is fully `asyncio`. Blocking I/O runs in `ThreadPoolExecutor`s.

```
DataCubeLoader.iter_datacubes()  ->  CubeSites.process_datacubes()  ->  Converter._async_save()
   (loader.py)                         (stations.py)                      (convert.py -> CubeFile.save)
```

- **`convert.py`**: `Converter` is the root pydantic config: loader, stations,
  output path/template, record length, Steim level and write threads.
  `Converter.load()` wires up the sidecar files. The default executor size
  comes from `SLURM_CPUS_PER_TASK`, falling back to `os.cpu_count()`. At
  import time, the pyrocko warnings *"Extrapolating GPS time information"*
  and *"No usable GPS timestamps found."* are **promoted to errors**. Those
  cubes then fail to load and are counted in `n_files_failed`.
- **`loader.py`**: `DataCubeLoader.prepare()` recursively scans `directories`
  and filters files by these checks:
  - the cube ID is the file extension, upper-cased, and must be in
    `cube_selection` when that is set
  - the file must be at least `min_file_size`
  - the mtime must fall within `start_date`/`end_date`, with a 60 s margin
  - `datacube.detect`
  - duplicate filenames are skipped

  `_fetch_datacubes()` runs `2 × loading_threads` worker tasks in a
  `TaskGroup`. They feed a bounded queue that provides backpressure, and
  `None` is the end sentinel. Workers call `files_queue.task_done()` only
  after the cube has been handed off. Keep that ordering, because
  `tests/test_loader.py` guards it.
- **`model/__init__.py`**: holds the following:
  - `CubeFile`, a dataclass with the path, Pyrocko traces and GPS tags of
    one datacube file.
    `from_file()` returns `None` on any exception or warning.
  - `CubeFile.save()` appends to miniSEED through `pyrocko.io.save`. The
    `year` and `julian_day` template keys both come from the cube mid-time.
    Writes take a lock per output file (`get_lock`), acquired in sorted
    order, because appends to the same SDS file must not race. It returns
    the number of bytes written.
  - `Location` (lat/lon/elevation/depth) with distance helpers.
- **`stations.py`**: `CubeSites` maps each cube ID to a list of `CubeLocation`
  entries, one per deployment period and position. For each cube, it derives
  a site from the GPS, merges it into an existing site within 25 m, or maps
  it to a `Station` through `StationMapper`. Next it finds the site that
  covers the cube start time and sets the NSL codes (`network`, the
  alphanumeric station name and the location). It then renames the channels
  and runs `post_processors`, which are keyed by **station name**.
  `exclude_stations` are skipped. Processors modify the cube in place.
  A location without a nearby station raises `StationNotFoundError`, and
  its files are skipped.
- **`station_mapper.py`**: `StationMapper` loads a GeoPackage through
  `fiona`. The `sensor_field`/`station_field` properties are configurable,
  and features with `status == "not_installed"` are ignored during
  validation. The mapper maps a site to the nearest feature within
  `distance_threshold_meters`. `channel_map` maps sensor types to cube
  channels (`p0/p1/p2` → e.g. `EPZ/EPN/EPE`).
  `station_orientation_overwrites` renames N/E to 1/2 and adds an azimuth
  offset.
- **`stationxml.py`**: `StationXML` builds an ObsPy `Inventory`. Responses
  come from the **NRL**, which needs network access and is cached with
  `lru_cache`. `station_responses` keys must match the sensor values in the
  GeoPackage. The defaults contain hard-coded ELISE project metadata:
  contacts, region and the `end_time` of 2023-09-01.
- **`processors/`**: `Processor` base class with `async process(cube) ->
  cube`. **When you add a processor**, export it in `processors/__init__.py`
  and add it to the `ProcessorType` union, which is currently only
  `FlipHorizontals`. Give each processor a distinct `name` literal so
  pydantic can tell them apart.
- **`stats.py`**: subclasses of `Stats` register themselves globally.
  `RuntimeStats.live_view()` renders all live instances as a `rich` panel.
  To show new metrics, subclass `Stats` and implement `_populate_table`.
- **`utils.py`**: `load_datacube()` is a lean copy of pyrocko's
  `datacube.iload`. It reuses the GPS tags from the data load instead of
  re-reading the whole file, and only scans neighbouring files when the
  file has no GPS fix. Its output must stay sample-identical to `iload`.
  `get_elevation()` queries the OpenTopoData API, rate-limited
  to 1 request/s. `format_bytes` and `DATETIME_MAX` are also defined here.
- **`plot.py`**: an ad-hoc diagnostic. It writes `~/plot-out.csv` **at import
  time** and has a hard-coded end date.

## Conventions

- Configuration lives in pydantic v2 models (`BaseModel`) that round-trip to
  JSON. Runtime state goes in `PrivateAttr`s with a leading underscore.
  Prefer adding config fields with sensible defaults, so that `dcube init`
  stays usable.
- Use module-level `logger = logging.getLogger(__name__)` with lazy `%`
  formatting.
- Use `from __future__ import annotations` and put type-only imports under
  `TYPE_CHECKING`.
- The CLI imports heavy modules lazily inside each subcommand branch.

## Tests

- `tests/test_loader.py` uses `pytest-asyncio` (`asyncio_mode = "auto"`), so
  plain `async def test_*` functions work.
- Unit tests monkeypatch `CubeFile.from_file` and build loaders with
  `make_loader()`.
- `test_load_real_datacubes` reads real data from
  `/project/eifel-largen/data-elise/...`. It is skipped when that path is
  missing, and takes about 20 s when present.
- Real datacube files are large, at roughly 60 MB+, so do not commit any
  test data.
