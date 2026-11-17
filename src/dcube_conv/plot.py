from __future__ import annotations

from itertools import cycle

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from dcube_conv.stations import Deployments


def plot_cube_coverage(deployments: Deployments) -> None:
    """Plot the deployments of each cube over time, colored by station."""
    _fig, ax = plt.subplots()

    cubes = sorted({d.cube_id for d in deployments.iter_deployments()})
    colors = dict(
        zip(
            sorted({d.station_code for d in deployments.iter_deployments()}),
            cycle(mcolors.TABLEAU_COLORS),
            strict=False,
        )
    )
    for deployment in deployments.iter_deployments():
        ax.add_patch(
            Rectangle(
                (deployment.start_time, cubes.index(deployment.cube_id)),
                deployment.end_time - deployment.start_time,
                1,
                ec="k",
                lw=0.5,
                fc=colors[deployment.station_code] if deployment.station else "w",
            )
        )

    ax.set_yticks(range(len(cubes)))
    ax.set_yticklabels(cubes)

    ax.autoscale_view()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))

    plt.show()
