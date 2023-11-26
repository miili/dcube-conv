import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self

from pyrocko.io import datacube, save
from pyrocko.trace import Trace

logger = logging.getLogger(__name__)

SteimCompression = Literal[None, 1, 2]
RecordLength = Literal[512, 1024, 2048, 4096, 8192]


@dataclass
class DatacubeTraces:
    path: Path
    traces: list[Trace]
    gps_tags: Any

    @classmethod
    def from_file(cls, file: Path) -> Self:
        logger.debug("Loading %s", file)
        data = [
            (tr, gps_tags)
            for (tr, gps_tags) in datacube.iload(str(file), yield_gps_tags=True)
        ]
        return cls(path=file, traces=[tr for (tr, _) in data], gps_tags=data[0][1])

    def save(
        self,
        output_path: Path,
        record_length: RecordLength = 4096,
        steim: SteimCompression = 1,
    ) -> None:
        logger.info("Saving miniSeed %s", output_path)
        save(self.traces, str(output_path), record_length=record_length, steim=steim)
