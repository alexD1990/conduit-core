# tests/test_batch_processing.py

import pytest
import time
from typing import Iterable, Dict, Any, Optional

from conduit_core.connectors.base import BaseSource, BaseDestination
from conduit_core.config import IngestConfig, Source, Destination, Resource
from conduit_core.engine import run_resource
from conduit_core.batch import read_in_batches

# --- Mock Connectors ---

class MockSource(BaseSource):
    def __init__(self, config, num_records=10):
        super().__init__(config)
        self.num_records = num_records

    def read(self, query: str = None) -> Iterable[Dict[str, Any]]:
        for i in range(self.num_records):
            yield {'id': i, 'data': f'record_{i}'}
            time.sleep(0.001) # Simulate some I/O delay

    def estimate_total_records(self) -> Optional[int]:
        return self.num_records


class MockSlowDestination(BaseDestination):
    def __init__(self, config, write_delay=0.01):
        super().__init__(config)
        self.write_delay = write_delay
        self.records_written = 0

    def write(self, records: Iterable[Dict[str, Any]]):
        batch = list(records)
        time.sleep(self.write_delay * len(batch))
        self.records_written += len(batch)

# --- Tests ---

def test_read_in_batches_correct_sizes():
    """Test at read_in_batches gir riktig batch-størrelse."""
    source_data = [{'id': i} for i in range(105)]
    batches = list(read_in_batches(iter(source_data), batch_size=10))
    
    assert len(batches) == 11 # 10 batches of 10, 1 batch of 5
    assert all(len(b) == 10 for b in batches[:-1])
    assert len(batches[-1]) == 5

def test_read_in_batches_empty_source():
    """Test at read_in_batches håndterer tom kilde."""
    source_data = []
    batches = list(read_in_batches(iter(source_data), batch_size=10))
    assert len(batches) == 0

def test_read_in_batches_smaller_than_batch_size():
    """Test når kilden har færre records enn batch_size."""
    source_data = [{'id': i} for i in range(5)]
    batches = list(read_in_batches(iter(source_data), batch_size=10))
    assert len(batches) == 1
    assert len(batches[0]) == 5

def test_engine_uses_batch_processing(monkeypatch, tmp_path):
    """Test at engine bruker batch processing korrekt."""
    import conduit_core.connectors.registry as registry

    original_source_map = registry._SOURCE_CONNECTOR_MAP
    original_dest_map = registry._DESTINATION_CONNECTOR_MAP

    # Create a destination instance that we can inspect
    batches_received = []

    class InspectableDestination(BaseDestination):
        # *** FIX: Add __init__ to accept config ***
        def __init__(self, config):
            super().__init__(config)
            
        def write(self, records):
            # Store each batch that comes in
            batch = list(records)
            batches_received.append(batch)

        # Remove write_one to force batch processing
        def __getattribute__(self, name):
            if name == 'write_one':
                raise AttributeError("write_one not supported")
            return super().__getattribute__(name)

    # Mock connector registry
    def create_source(config):
        return MockSource(config, num_records=2500)

    # *** FIX: Modify create_dest to pass config ***
    def create_dest(config):
        return InspectableDestination(config) # Pass config here

    registry._SOURCE_CONNECTOR_MAP = {'mocksource': create_source}
    registry._DESTINATION_CONNECTOR_MAP = {'mockdest': create_dest}

    try:
        config = IngestConfig(
            sources=[Source(name='test_source', type='mocksource')],
            destinations=[Destination(name='test_dest', type='mockdest')],
            resources=[Resource(
                name='test_resource',
                source='test_source',
                destination='test_dest',
                query='SELECT 1'
            )]
        )

        import os
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # Kjør med batch_size=1000
            run_resource(config.resources[0], config, batch_size=1000)

            # Verifiser at data ble sendt i batches
            assert len(batches_received) == 3 # 2 batches of 1000, 1 batch of 500
            assert len(batches_received[0]) == 1000
            assert len(batches_received[1]) == 1000
            assert len(batches_received[2]) == 500

            # Verifiser at alle records ble mottatt
            total_received = sum(len(batch) for batch in batches_received)
            assert total_received == 2500

        finally:
            os.chdir(original_cwd)

    finally:
        # Gjenopprett original registry
        registry._SOURCE_CONNECTOR_MAP = original_source_map
        registry._DESTINATION_CONNECTOR_MAP = original_dest_map


def test_batch_processing_memory_efficiency():
    """Test at batch processing ikke laster alt i minnet."""
    # Denne testen verifiserer at vi kun holder én batch om gangen

    max_memory_used = []

    class MemoryTrackingDestination(BaseDestination):
        # *** FIX: Add __init__ to accept config ***
        def __init__(self, config):
            super().__init__(config)
            
        def write(self, records):
            # Simuler at vi tracker hvor mye som er i minnet
            batch = list(records) # Consume the iterator
            batch_size = len(batch)
            max_memory_used.append(batch_size)

    # *** FIX: Pass config (can be None for this test) ***
    dest = MemoryTrackingDestination(config=None)
    source_data = ({'id': i} for i in range(500)) # Generator, not list

    # Simulate engine's processing loop manually
    for batch in read_in_batches(source_data, batch_size=100):
        dest.write(batch)

    # Assert that the destination only saw batches of size 100
    assert len(max_memory_used) == 5
    assert all(size == 100 for size in max_memory_used)