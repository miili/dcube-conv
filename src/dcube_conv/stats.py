from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, ClassVar, NoReturn, Type
from weakref import WeakValueDictionary

from pydantic import BaseModel, PrivateAttr
from rich import box
from rich.console import Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskProgressColumn,
    TextColumn,
)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from dcube_conv.utils import format_bytes

logger = logging.getLogger(__name__)

STATS_CLASSES: set[Type[Stats]] = set()
STATS_INSTANCES: WeakValueDictionary[str, Stats] = WeakValueDictionary()

ACCENT = "cyan"
MUTED = "grey50"
BORDER = "grey35"


def fmt_int(value: int) -> str:
    return f"{value:,}"


def fmt_duration(seconds: float) -> str:
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:d}:{minutes:02d}:{seconds:02d}"


class EtaColumn(ProgressColumn):
    """Remaining time while running, elapsed time when finished."""

    def render(self, task: Task) -> Text:
        if task.finished:
            return Text(f"done in {fmt_duration(task.finished_time or 0)}", "green")
        remaining = task.time_remaining
        if remaining is None:
            return Text("eta -:--:--", MUTED)
        return Text(f"eta {fmt_duration(remaining)}", MUTED)


def make_progress() -> Progress:
    return Progress(
        SpinnerColumn("dots", style=ACCENT, finished_text="[green]✔"),
        TextColumn("[bold]{task.description:<10}"),
        BarColumn(
            bar_width=None,
            style="grey23",
            complete_style=ACCENT,
            finished_style="green",
        ),
        TaskProgressColumn(),
        TextColumn("[grey50]{task.fields[detail]}"),
        EtaColumn(),
        expand=True,
    )


class StatsTable:
    """Rows of a dashboard section: label, count, data size and details.

    Columns have fixed widths, so numbers line up across sections.
    """

    def __init__(self) -> None:
        self.table = Table.grid(expand=True, padding=(0, 2))
        self.table.add_column(style=MUTED, no_wrap=True, width=12)
        self.table.add_column(justify="right", no_wrap=True, width=9)
        self.table.add_column(justify="right", no_wrap=True, width=10)
        self.table.add_column(no_wrap=True, overflow="ellipsis", ratio=1)

    def row(
        self,
        label: str,
        value: int | str = "",
        style: str = "bold",
        size: int | None = None,
        detail: str = "",
    ) -> None:
        text = fmt_int(value) if isinstance(value, int) else value
        self.table.add_row(
            f" {label}",
            Text(text, style=style),
            fmt_size(size) if size is not None else "",
            Text.from_markup(detail, style=MUTED),
        )


def fmt_size(size: float) -> Text:
    """Data size with a muted unit, e.g. 23.9 TB."""
    number, _, unit = format_bytes(size).partition(" ")
    return Text.assemble((number, "bold"), (f" {unit}", MUTED))


def problem(value: int, label: str, style: str = "yellow") -> str:
    """Detail markup of a count, highlighted when non-zero."""
    return f"[{style}]{value:,} {label}[/]" if value else f"{value:,} {label}"


def percent(part: float, whole: float) -> str:
    return f"{part / whole:.1%}" if whole else "-"


class Stats(BaseModel):
    title: ClassVar[str] = ""
    _position: int = PrivateAttr(10)

    def __init_subclass__(cls: Type[Stats], **kwargs) -> None:
        STATS_CLASSES.add(cls)

    def model_post_init(self, __context: Any) -> None:
        STATS_INSTANCES[self.__class__.__name__] = self

    def progress_bar(self) -> RenderableType | None:
        """Progress bar shown above the table."""
        return None

    def _populate(self, table: StatsTable) -> None: ...


class RuntimeStats:
    @staticmethod
    def render(time_started: float) -> RenderableType:
        stats = sorted(STATS_INSTANCES.values(), key=lambda s: s._position)

        parts: list[RenderableType] = [
            p for s in stats if (p := s.progress_bar()) is not None
        ]
        parts.append(Text())
        for s in stats:
            table = StatsTable()
            s._populate(table)
            parts.append(
                Rule(
                    Text(f" {s.title.upper()} ", style=f"bold {ACCENT}"),
                    align="left",
                    style=BORDER,
                )
            )
            parts.append(table.table)

        elapsed = fmt_duration(time.monotonic() - time_started)
        return Panel(
            Group(*parts),
            title=f"[bold {ACCENT}]◆[/] [bold]dcube[/]  [{MUTED}]DATA-CUBE³ → miniSEED",
            title_align="left",
            subtitle=f"[{MUTED}]elapsed[/] [bold]{elapsed}[/]",
            subtitle_align="right",
            box=box.ROUNDED,
            border_style=BORDER,
            padding=(0, 2),
        )

    @classmethod
    async def live_view(cls) -> NoReturn:
        time_started = time.monotonic()
        with Live(cls.render(time_started), refresh_per_second=4) as live:
            while True:
                await asyncio.sleep(0.25)
                live.update(cls.render(time_started))
