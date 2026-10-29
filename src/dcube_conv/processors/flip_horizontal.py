from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import numpy as np

from dcube_conv.processors.base import Processor

if TYPE_CHECKING:
    from dcube_conv.model import CubeTraces


logger = logging.getLogger(__name__)


class FlipHorizontals(Processor):
    name: str = "FlipHorizontals"

    async def process(self, data: CubeTraces) -> CubeTraces:
        for tr in data.traces:
            if tr.channel.endswith("Z"):
                continue
            logger.info(
                "Flipping horizontal traces by 180 deg: %s.%s.%s",
                tr.network,
                tr.station,
                tr.channel,
            )
            if tr.ydata is not None:
                tr.ydata = await asyncio.to_thread(np.negative, tr.ydata)
        return data
