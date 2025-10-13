# src/conduit_core/logging_utils.py

import time
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
    
    def error(self, message: str):
        """Logger en error (rød)."""
        timestamp = self._get_timestamp()
        
        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("ERROR ", style="bold red")
        text.append(message, style="red")
        
        console.print(text)
    
    def batch_progress(self, batch_num: int, records_in_batch: int, total_so_far: int):
        """Logger progress for en batch."""
        timestamp = self._get_timestamp()
        
        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append(f"Batch {batch_num} ", style="cyan")
        text.append(f"processed {records_in_batch} records ", style="white")
        text.append(f"(total: {total_so_far})", style="dim")
        
        console.print(text)
    
    def complete_resource(self, total_records: int, successful: int, failed: int):
        """Logger completion av en resource."""
        if not self.start_time:
            return
        
        elapsed = time.time() - self.start_time
        timestamp = self._get_timestamp()
        
        text = Text()
        text.append(f"{timestamp} ", style="dim")
        text.append("DONE ", style="bold green")
        text.append(f"resource {self.resource_name} ", style="bold")
        text.append(f"[in {elapsed:.2f}s]", style="dim green")
        
        console.print(text)
        
        # Summary line
        summary = Text()
        summary.append(f"{timestamp} ", style="dim")
        summary.append("     → ", style="dim")
        summary.append(f"{successful} ", style="green")
        summary.append("successful, ", style="white")
        
        if failed > 0:
            summary.append(f"{failed} ", style="red")
            summary.append("failed", style="white")
        else:
            summary.append("0 failed", style="dim")
        
        console.print(summary)
        console.print()  # Empty line
    
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
    text.append("Completed successfully! ", style="bold green")
    text.append(f"Ran {total_resources} resource(s) ", style="white")
    text.append(f"in {total_time:.2f}s", style="dim")
    console.print(text)
    console.print()