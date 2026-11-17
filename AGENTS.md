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
| `dcube locate run.json` | Scan headers and (re-)locate sessions into `run.deployments.json`, without converting data or touching `run.progress` |
| `dcube stationxml init > sx.json` | Print the default `StationXML` config |
| `dcube stationxml create [--fill-elevations] sx.json run.json` | Write `sx.xml` and `sx.missing.csv` from the run's deployments |
| `dcube plot run.json` | Plot deployments per cube over time (exploratory) |

`-v` sets the log level to DEBUG and turns on asyncio debug mode.

### Files written next to the config (`run.json`)

- `run.deployments.json`: the located deployments (state). It is reloaded on
  the next run, so sessions are located only once, and written atomically.
  The derived `.csv` and `.yaml` (Pyrocko stations) views are written at the
  end of the run. The `stations` section of `run.json` stays the
  configuration; it is never overwritten by the state.
- `run.progress`: one converted file path per line. Files listed here, and
  copies of them, are skipped on the next run, which makes conversion
  **resumable**. Delete it to re-convert.
- `run.log`: the log file. It ends with a summary that accounts for every
  file (written, failed to load, without station, excluded) and the data
  sizes and read/write rates.

## Data model

Datacube files → recording sessions → deployments → stations:

- A **datacube file** (`CubeFile`) holds about one day of one cube. Its header
  (`DataCubeHeader`) gives the device, session start, file number, sampling
  rate and gain. File names repeat every year, so files are identified by
  their header (`recording_id`), never by name.
- A **recording session** is everything a cube records between switching on
  and off (`DataCubeHeader.session_id`). A cube does not move within a
  session: GPS positions of a session agree within about 10 m.
- A **deployment** (`Deployment`) is one located recording session: position,
  time span, sampling rate and gain, and the matched layout station.
- A **station** (`Station`) is a named point of the planned layout in the
  GeoPackage. `Deployments.iter_stations()` yields each station with the
  deployments that recorded it (`StationDeployments`).

## Architecture (`src/dcube_conv/`)

The pipeline is fully `asyncio`. Blocking I/O runs in `ThreadPoolExecutor`s.

```
DataCubeLoader.prepare()  ->  Deployments.prepare(loader.sessions)          (scan, locate)
DataCubeLoader.iter_datacubes()  ->  Deployments.process_datacubes()  ->  Converter._async_save()
```

- **`convert.py`**: `Converter` is the root pydantic config: loader, stations
  (`Deployments`), output path/template, record length, Steim level and
  write threads. `Converter.load()` wires up the sidecar files. The default
  executor size comes from `SLURM_CPUS_PER_TASK`, falling back to
  `os.cpu_count()`. At import time, the pyrocko warnings *"Extrapolating GPS
  time information"* and *"No usable GPS timestamps found."* are **promoted
  to errors**. Those cubes then fail to load and are counted in
  `n_files_failed`.
- **`loader.py`**: `DataCubeLoader.prepare()` recursively scans `directories`
  and reads each file header (`detect_datacube`). It filters by
  `cube_selection` (file extension), `min_file_size` and the header date
  within `start_date`/`end_date` (mtimes are unreliable, they change on
  copy). Copies of a recording keep the largest file. `sessions` holds all
  recordings in range per session, including already converted ones.
  `_fetch_datacubes()` runs `2 × loading_threads` worker tasks in a
  `TaskGroup`. They feed a bounded queue that provides backpressure, and
  `None` is the end sentinel. Workers call `files_queue.task_done()` only
  after the cube has been handed off. Keep that ordering, because
  `tests/test_loader.py` guards it.
- **`model/__init__.py`**: holds the following:
  - `CubeFile`: path, header, Pyrocko traces, GPS tags, its GPS `location`
    (None without fix) and, once assigned, its `deployment`.
    `from_file()` returns `None` on any exception or warning.
  - `CubeFile.save()` appends to miniSEED through `pyrocko.io.save`. The
    `year` and `julian_day` template keys both come from the cube mid-time.
    Writes take a lock per output file (`get_lock`), acquired in sorted
    order, because appends to the same SDS file must not race. It returns
    the number of bytes written.
  - `Location` (lat/lon/elevation/depth) with distance helpers and
    `from_gps_tags()`, the median position of tags with a GPS fix.
- **`stations.py`**: `Deployments.prepare()` locates new sessions in threads
  (`locate_session` reads the GPS tags of the last file, and of earlier
  files until one has a fix) and matches every deployment to the nearest
  layout station. The session end comes from the last GPS tags
  (`gps_end_time`): the cube clock drifts by up to about 30 s per day, so
  the nominal end from the file date and sample count is up to an hour late
  and overlaps the next session. Deployments in the state with an older
  `locate_version` are located again. Sessions without any GPS
  fix are skipped and retried next run. `process_datacubes()` assigns each
  file by `session_id`, sets the NSL codes (`network`, the alphanumeric
  station code and the location code), renames the channels and runs
  `post_processors`, which are keyed by **station code** and modify the
  cube in place. Files of `exclude_stations` are skipped; files more than
  `distance_threshold_meters` from their deployment are counted "off site".
- **`station_mapper.py`**: `StationMapper` loads a GeoPackage through
  `fiona`. The `sensor_field`/`station_field` properties are configurable,
  and features with `status == "not_installed"` are ignored during
  validation. `get_station()` returns the nearest feature within
  `distance_threshold_meters`. `channel_map` maps sensor types to cube
  channels (`p0/p1/p2` → e.g. `EPZ/EPN/EPE`).
  `station_orientation_overwrites` renames N/E to 1/2 and adds an azimuth
  offset.
- **`stationxml.py`**: `StationXML` builds an ObsPy `Inventory`.
  `channel_epochs()` merges consecutive deployments of a station into one
  channel epoch while gain and sampling rate stay the same and the position
  stays within `merge_distance_meters` (15 m) of the epoch's mean position.
  Restarts and cube swaps at service visits therefore do not split epochs,
  the datalogger has no serial number. Epochs take the mean position of
  their deployments and are clipped so they never overlap, because the next
  cube is often switched on before the last one is stopped. Responses come
  from a local **NRL v2** copy (`nrl_root`, default `~/.cache/NRL`);
  datalogger keys are
  `("DiGOSOmnirecs", "DataCube", gain, "<rate> Hz")`. `fix_units` fills the
  missing units of the NRL v2 preamplifier stage and the sensitivity is
  always recalculated. `station_responses` keys must match the sensor values
  in the GeoPackage and use NRL v2 sensor keys. The defaults contain
  hard-coded ELISE project metadata: contacts and region.
- **`processors/`**: `Processor` base class with `async process(cube) ->
  None`, modifying the cube in place. **When you add a processor**, export it
  in `processors/__init__.py` and add it to the `ProcessorType` union, which
  is currently only `FlipHorizontals`. Give each processor a distinct `name`
  literal so pydantic can tell them apart.
- **`stats.py`**: subclasses of `Stats` register themselves globally.
  `RuntimeStats.live_view()` renders a compact `rich` dashboard that must fit
  short terminals: progress bars from `progress_bar()` and one table with a
  few dense rows per stats object from `_populate()` (section, label, count,
  data size, details). Sections follow the data flow: Files, Deployments,
  Output. Prefer adding details to an existing row over adding rows.
- **`utils.py`**: `detect_datacube()` parses a file header into a
  `DataCubeHeader` (raises `DataCubeError` otherwise) and corrects the GPS
  week rollover. `load_datacube()` is a lean copy of pyrocko's
  `datacube.iload`. It reuses the GPS tags from the data load instead of
  re-reading the whole file, and only scans neighbouring files when the
  file has no GPS fix. Its output must stay sample-identical to `iload`,
  with one exception: it only applies the week rollover correction to times
  before the last rollover (2019). A single bogus GPS-UTC offset in a file
  (e.g. 5 s instead of 18 s, seen on cube 274) makes `iload` shift a 2025
  file to 2045.
  `get_elevation()` queries the OpenTopoData API, rate-limited to 1
  request/s.
- **`plot.py`**: an ad-hoc diagnostic of deployments per cube.

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
