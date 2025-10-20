# src/conduit_core/logging_utils.py

import time
import traceback
from datetime import datetime
from rich.console import Console
from rich.text import Text
from typing import Optional

console = Console()


class ConduitLogger:
    """dbt-style logger for Conduit Core."""

    def __init__(self, resource_name: str):
        self.resource_name = resource_name
        self.start_time = None
        self.operation_start = None

    def _get_timestamp(self) -> str:
        """Returnerer formatert timestamp."""
        return datetime.now().strftime("%H:%M:%S")

    def _elapsed_time(self) -> str:
        """Returnerer elapsed time siden start."""
        if self.start_time:
            elapsed = time.time() - self.start_time
            return f"[{elapsed:.2f}s]"
        return ""

    def start_resource(self):
        """Logger start av en resource."""
        self.start_time = time.time()
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("START ", style="bold cyan")
        text.append(f"resource {self.resource_name}", style="bold")

        console.print(text)

    def info(self, message: str, prefix: str = ""):
        """Logger en info-melding."""
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        if prefix:
            text.append(f"{prefix} ", style="cyan")
        text.append(message)

        console.print(text)

    # --- New Debug Method ---
    def debug(self, message: str, prefix: str = "DEBUG"):
        """Logger en debug-melding (dimmed)."""
        # For now, print debug messages similar to info but dimmed.
        # Could add logic later to only show if a --debug flag is set.
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        if prefix:
            text.append(f"[{prefix}] ", style="dim cyan")
        text.append(message, style="dim")

        console.print(text)
    # --- End New Debug Method ---

    def success(self, message: str, timing: Optional[float] = None):
        """Logger en success-melding (grønn)."""
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("OK ", style="bold green")
        text.append(message)

        if timing:
            text.append(f" [in {timing:.2f}s]", style="dim green")

        console.print(text)

    def warning(self, message: str):
        """Logger en warning (gul)."""
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("WARN ", style="bold yellow")
        text.append(message, style="yellow")

        console.print(text)

    def error(self, message: str, exc_info: bool = False):
        """Logger en error (rød), optionally including traceback."""
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("ERROR ", style="bold red")
        text.append(message, style="red")

        console.print(text)

        if exc_info:
             console.print_exception(show_locals=False)

    def batch_progress(self, batch_num: int, records_in_batch: int, total_so_far: int):
        """Logger progress for en batch."""
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append(f"Batch {batch_num} ", style="cyan")
        text.append(f"processed {records_in_batch} records ", style="white")
        text.append(f"(total: {total_so_far})", style="dim")

        console.print(text)

    def complete_resource(self, total_processed: int, successful: int, failed: int, dry_run: bool = False):
        """Logger completion av en resource."""
        if not self.start_time:
            return

        elapsed = time.time() - self.start_time
        timestamp = self._get_timestamp()

        text = Text()
        text.append(f"{timestamp} ", style="dim")

        status_icon = "[OK]"
        status_style = "bold green"
        if failed > 0:
             status_icon = "[WARN]"
             status_style = "bold yellow" if successful > 0 else "bold red"
        elif total_processed == 0 and successful == 0:
             status_icon = "-"
             status_style = "dim"


        if dry_run:
            status_icon = "🔍"
            status_style = "bold yellow"
            text.append(f"{status_icon} [DRY RUN] DONE ", style=status_style)
        else:
            text.append(f"{status_icon} DONE ", style=status_style)

        text.append(f"resource {self.resource_name} ", style="bold")
        text.append(f"[in {elapsed:.2f}s]", style="dim")

        console.print(text)

        summary = Text()
        summary.append(f"{timestamp} ", style="dim")
        summary.append("      → ", style="dim")

        if dry_run:
            summary.append(f"{successful} would be written", style="yellow")
            if failed > 0:
                 summary.append(f", {failed} failed quality checks", style="yellow")
        else:
            summary.append(f"{successful} successful, ", style="white")

            if failed > 0:
                summary.append(f"{failed} failed", style="red")
            else:
                summary.append("0 failed", style="dim")

        console.print(summary)
        console.print()

    def separator(self):
        """Printer en separator linje."""
        console.print("─" * 80, style="dim")


def print_header():
    """Printer Conduit Core header."""
    console.print()
    console.print("🚀 [bold cyan]Conduit Core[/bold cyan]", justify="left")
    console.print("   Data ingestion pipeline starting...", style="dim")
    console.print()


def print_summary(total_resources: int, total_time: float):
    """Printer final summary."""
    console.print()
    console.print("─" * 80, style="dim")
    text = Text()
    # TODO: Add logic to check manifest for overall status (success, partial, fail)
    text.append("Completed successfully! ", style="bold green") # Assume success for now
    text.append(f"Ran {total_resources} resource(s) ", style="white")
    text.append(f"in {total_time:.2f}s", style="dim")
    console.print(text)
    console.print()