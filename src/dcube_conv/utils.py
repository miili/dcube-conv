from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Literal

import aiohttp


def datetime_now() -> datetime:
    return datetime.now(tz=timezone.utc)


ElevationModel = Literal[
    "aster30m",
    "etopo1",
    "eudem25m",
    "mapzen",
    "ned10m",
    "nzdem8m",
    "srtm30m",
    "emod2018",
    "gebco2020",
    "bkg200m",
    "swisstopo-2m",
]

LAST_REQUEST = datetime_now()


async def get_elevation(
    lat: float,
    lon: float,
    model: ElevationModel = "aster30m",
    interpolation: Literal["nearest", "bilinear", "cubic"] = "bilinear",
) -> float:
    """Get elevation from OpenTopoData API.

    Args:
        lat (float): Latitude in [deg].
        lon (float): Longitude in [deg].
        model (ElevationModel, optional): The elevation model to use.
            Defaults to "aster30m".
        interpolation (Literal["nearest", "bilinear", "cubic"], optional):
            The interpolation method. Defaults to "bilinear".

    Returns:
        float: The elevation in [m].
    """
    global LAST_REQUEST
    if (datetime_now() - LAST_REQUEST).total_seconds() < 1.0:
        await asyncio.sleep(1.0)

    async with (
        aiohttp.ClientSession() as session,
        session.get(
            f"https://api.opentopodata.org/v1/{model}",
            params={
                "locations": f"{lat},{lon}",
                "interpolation": interpolation,
            },
        ) as response,
    ):
        LAST_REQUEST = datetime_now()
        data = await response.json()
        return data["results"][0]["elevation"]
