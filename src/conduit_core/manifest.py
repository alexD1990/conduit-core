"""Pipeline manifest for audit trail and lineage tracking."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class ManifestEntry:
    """Single pipeline execution entry."""

    pipeline_name: str
    source_type: str
    destination_type: str
    started_at: str
    completed_at: str
    status: str  # success, failed, partial
    records_read: int
    records_written: int
    records_failed: int
    duration_seconds: float
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class PipelineManifest:
    """Manages pipeline execution history and audit trail."""

    def __init__(self, manifest_path: Optional[Path] = None):
        self.manifest_path = manifest_path or Path("manifest.json")
        self.entries: List[ManifestEntry] = []
        self._load()

    def _load(self) -> None:
        """Load existing manifest from disk."""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r') as f:
                data = json.load(f)
                self.entries = [ManifestEntry(**entry) for entry in data.get("runs", [])]

    def _save(self) -> None:
        """Save manifest to disk."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "version": "1.0",
            "runs": [asdict(entry) for entry in self.entries]
        }

        with open(self.manifest_path, 'w') as f:
            json.dump(data, f, indent=2)

    def add_entry(self, entry: ManifestEntry) -> None:
        """Add a new execution entry."""
        self.entries.append(entry)
        self._save()

    def get_latest(self, pipeline_name: str) -> Optional[ManifestEntry]:
        """Get the most recent run for a pipeline."""
        pipeline_runs = [e for e in self.entries if e.pipeline_name == pipeline_name]
        return pipeline_runs[-1] if pipeline_runs else None

    def get_all(self, pipeline_name: Optional[str] = None) -> List[ManifestEntry]:
        """Get all runs, optionally filtered by pipeline name."""
        if pipeline_name:
            return [e for e in self.entries if e.pipeline_name == pipeline_name]
        return self.entries

    def get_failed_runs(self) -> List[ManifestEntry]:
        """Get all failed pipeline runs."""
        return [e for e in self.entries if e.status == "failed"]


class ManifestTracker:
    """Context manager for tracking pipeline execution."""

    def __init__(
        self,
        manifest: PipelineManifest,
        pipeline_name: str,
        source_type: str,
        destination_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.manifest = manifest
        self.pipeline_name = pipeline_name
        self.source_type = source_type
        self.destination_type = destination_type
        self.metadata = metadata or {}

        self.started_at: Optional[datetime] = None
        self.records_read = 0
        self.records_written = 0
        self.records_failed = 0
        self.error_message: Optional[str] = None

    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        completed_at = datetime.now(timezone.utc)
        duration = (completed_at - self.started_at).total_seconds()

        status = "success"
        if exc_type is not None:
            status = "failed"
            self.error_message = str(exc_val)
        elif self.records_failed > 0:
            status = "partial"

        entry = ManifestEntry(
            pipeline_name=self.pipeline_name,
            source_type=self.source_type,
            destination_type=self.destination_type,
            started_at=self.started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            status=status,
            records_read=self.records_read,
            records_written=self.records_written,
            records_failed=self.records_failed,
            duration_seconds=duration,
            error_message=self.error_message,
            metadata=self.metadata
        )

        self.manifest.add_entry(entry)

        return False  # Don't suppress exceptions