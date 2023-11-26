import logging
from pathlib import Path
from typing import Literal, Self

from pydantic.dataclasses import dataclass
from pyrocko.io import datacube, save
from pyrocko.trace import Trace

logger = logging.getLogger(__name__)

SteimCompression = Literal[None, 1, 2]


@dataclass
class DatacubeTraces:
    path: Path
    traces: list[Trace]

    @classmethod
    def from_file(cls, file: Path) -> Self:
        logger.debug("Loading %s", file)
        return cls(path=file, traces=[tr for tr in datacube.iload(str(file))])

    def save(
        self,
        output_path: Path,
        record_length: int = 4096,
        steim: SteimCompression = 1,
    ) -> None:
        logger.info("Saving miniSeed %s", output_path)
        save(str(output_path), self.traces)
