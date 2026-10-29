from __future__ import annotations

from datetime import datetime, timezone
from itertools import cycle
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from dcube_conv.stations import CubeSite, CubeSites

cycol = cycle(mcolors.TABLEAU_COLORS)
OUT = Path("~/plot-out.csv").expanduser()
OUT.write_text("cube_id,lat,lon\n")


def site_coverage_rectangle(site: CubeSite, y_coord: float) -> None | Rectangle:
    if site.start_time == site.end_time:
        print("Zero length site", site)
    if site.station is None:
        print("Site without station", site)
        with OUT.open("a") as f:
            f.write(f"{site.cube_id},{site.lat},{site.lon}\n")
    else:
        return None
    return Rectangle(
        (site.start_time, y_coord),
        site.end_time - site.start_time,
        1,
        ec="k",
        lw=0.5,
        fc=next(cycol),
    )


def plot_cube_coverage(cube_sites: CubeSites) -> None:
    cube_sites.fill_endtimes(datetime(2023, 9, 1, tzinfo=timezone.utc))

    _fig, ax = plt.subplots()

    cubes = list(cube_sites.sites.keys())

    for site in cube_sites.iter_sites():
        y_loc = cubes.index(site.cube_id)
        rect = site_coverage_rectangle(site, y_loc)
        if rect is not None:
            ax.add_patch(rect)

    ax.set_yticks(range(cube_sites.n_cubes))
    ax.set_yticklabels(cubes)

    ax.autoscale_view()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))

    plt.show()
