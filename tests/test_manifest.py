"""Tests for pipeline manifest."""

import json
from pathlib import Path
from datetime import datetime

import pytest

from conduit_core.manifest import PipelineManifest, ManifestEntry, ManifestTracker


@pytest.fixture
def manifest_path(tmp_path):
    """Temporary manifest file path."""
    return tmp_path / "manifest.json"


@pytest.fixture
def manifest(manifest_path):
    """Create a fresh manifest."""
    return PipelineManifest(manifest_path)


def test_manifest_entry_creation():
    """Test creating a manifest entry."""
    entry = ManifestEntry(
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    assert entry.pipeline_name == "test_pipeline"
    assert entry.status == "success"
    assert entry.records_read == 100


def test_manifest_add_entry(manifest, manifest_path):
    """Test adding entries to manifest."""
    entry = ManifestEntry(
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    manifest.add_entry(entry)
    
    assert len(manifest.entries) == 1
    assert manifest_path.exists()


def test_manifest_persistence(manifest_path):
    """Test that manifest persists across instances."""
    manifest1 = PipelineManifest(manifest_path)
    
    entry = ManifestEntry(
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    manifest1.add_entry(entry)
    
    # Create new instance
    manifest2 = PipelineManifest(manifest_path)
    
    assert len(manifest2.entries) == 1
    assert manifest2.entries[0].pipeline_name == "test_pipeline"


def test_manifest_get_latest(manifest):
    """Test getting latest run for a pipeline."""
    entry1 = ManifestEntry(
        pipeline_name="pipeline_a",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    entry2 = ManifestEntry(
        pipeline_name="pipeline_a",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-02T00:00:00",
        completed_at="2025-01-02T00:01:00",
        status="success",
        records_read=200,
        records_written=200,
        records_failed=0,
        duration_seconds=60.0
    )
    
    manifest.add_entry(entry1)
    manifest.add_entry(entry2)
    
    latest = manifest.get_latest("pipeline_a")
    
    assert latest is not None
    assert latest.records_read == 200


def test_manifest_get_all_filtered(manifest):
    """Test getting all runs filtered by pipeline name."""
    entry1 = ManifestEntry(
        pipeline_name="pipeline_a",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    entry2 = ManifestEntry(
        pipeline_name="pipeline_b",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-02T00:00:00",
        completed_at="2025-01-02T00:01:00",
        status="success",
        records_read=200,
        records_written=200,
        records_failed=0,
        duration_seconds=60.0
    )
    
    manifest.add_entry(entry1)
    manifest.add_entry(entry2)
    
    pipeline_a_runs = manifest.get_all("pipeline_a")
    
    assert len(pipeline_a_runs) == 1
    assert pipeline_a_runs[0].pipeline_name == "pipeline_a"


def test_manifest_get_failed_runs(manifest):
    """Test getting failed runs."""
    entry1 = ManifestEntry(
        pipeline_name="pipeline_a",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-01T00:00:00",
        completed_at="2025-01-01T00:01:00",
        status="success",
        records_read=100,
        records_written=100,
        records_failed=0,
        duration_seconds=60.0
    )
    
    entry2 = ManifestEntry(
        pipeline_name="pipeline_b",
        source_type="csv",
        destination_type="postgresql",
        started_at="2025-01-02T00:00:00",
        completed_at="2025-01-02T00:01:00",
        status="failed",
        records_read=50,
        records_written=0,
        records_failed=50,
        duration_seconds=30.0,
        error_message="Connection failed"
    )
    
    manifest.add_entry(entry1)
    manifest.add_entry(entry2)
    
    failed = manifest.get_failed_runs()
    
    assert len(failed) == 1
    assert failed[0].status == "failed"
    assert failed[0].error_message == "Connection failed"


def test_manifest_tracker_success(manifest):
    """Test tracking successful pipeline execution."""
    with ManifestTracker(
        manifest=manifest,
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql"
    ) as tracker:
        tracker.records_read = 100
        tracker.records_written = 100
    
    assert len(manifest.entries) == 1
    entry = manifest.entries[0]
    
    assert entry.status == "success"
    assert entry.records_read == 100
    assert entry.records_written == 100
    assert entry.records_failed == 0


def test_manifest_tracker_failure(manifest):
    """Test tracking failed pipeline execution."""
    try:
        with ManifestTracker(
            manifest=manifest,
            pipeline_name="test_pipeline",
            source_type="csv",
            destination_type="postgresql"
        ) as tracker:
            tracker.records_read = 50
            raise ValueError("Test error")
    except ValueError:
        pass
    
    assert len(manifest.entries) == 1
    entry = manifest.entries[0]
    
    assert entry.status == "failed"
    assert entry.records_read == 50
    assert entry.error_message == "Test error"


def test_manifest_tracker_partial(manifest):
    """Test tracking partial success (some records failed)."""
    with ManifestTracker(
        manifest=manifest,
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql"
    ) as tracker:
        tracker.records_read = 100
        tracker.records_written = 90
        tracker.records_failed = 10
    
    assert len(manifest.entries) == 1
    entry = manifest.entries[0]
    
    assert entry.status == "partial"
    assert entry.records_written == 90
    assert entry.records_failed == 10


def test_manifest_tracker_metadata(manifest):
    """Test storing metadata with execution."""
    metadata = {"user": "test_user", "env": "production"}
    
    with ManifestTracker(
        manifest=manifest,
        pipeline_name="test_pipeline",
        source_type="csv",
        destination_type="postgresql",
        metadata=metadata
    ) as tracker:
        tracker.records_read = 100
        tracker.records_written = 100
    
    entry = manifest.entries[0]
    
    assert entry.metadata == metadata
    assert entry.metadata["user"] == "test_user"