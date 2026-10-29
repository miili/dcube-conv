from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from dcube_conv.model import CubeTraces


class Processor(BaseModel):
    name: str = "Processor"

    async def process(self, data: CubeTraces) -> CubeTraces: ...
