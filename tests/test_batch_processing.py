# tests/test_batch_processing.py

import pytest
from conduit_core.batch import read_in_batches, process_batches_with_callback
from conduit_core.connectors.base import BaseSource, BaseDestination
from conduit_core.config import Source, Destination, Resource, IngestConfig
from conduit_core.engine import run_resource


class MockSource(BaseSource):
    """Mock source som genererer et spesifikt antall records."""
    
    def __init__(self, config, num_records=2500):
        self.num_records = num_records
    
    def read(self, query=None):
        """Genererer num_records records."""
        for i in range(1, self.num_records + 1):
            yield {'id': i, 'value': f'record_{i}'}


class MockDestination(BaseDestination):
    """Mock destination som samler alle writes."""
    
    def __init__(self, config=None):
        self.batches_written = []
        self.total_records = 0
    
    def write(self, records):
        """Lagrer batch."""
        batch = list(records)
        self.batches_written.append(batch)
        self.total_records += len(batch)


def test_read_in_batches_basic():
    """Test at read_in_batches splitter data i riktige chunks."""
    # Generer 2500 records
    data = [{'id': i} for i in range(1, 2501)]
    
    batches = list(read_in_batches(iter(data), batch_size=1000))
    
    # Skal ha 3 batches: 1000, 1000, 500
    assert len(batches) == 3
    assert len(batches[0]) == 1000
    assert len(batches[1]) == 1000
    assert len(batches[2]) == 500
    
    # Sjekk at første og siste record er riktig
    assert batches[0][0]['id'] == 1
    assert batches[2][-1]['id'] == 2500


def test_read_in_batches_exact_multiple():
    """Test når antall records er eksakt delelig med batch_size."""
    data = [{'id': i} for i in range(1, 2001)]
    
    batches = list(read_in_batches(iter(data), batch_size=1000))
    
    # Skal ha nøyaktig 2 batches på 1000 hver
    assert len(batches) == 2
    assert len(batches[0]) == 1000
    assert len(batches[1]) == 1000


def test_read_in_batches_small_dataset():
    """Test med dataset mindre enn batch_size."""
    data = [{'id': i} for i in range(1, 101)]
    
    batches = list(read_in_batches(iter(data), batch_size=1000))
    
    # Skal ha én batch med 100 records
    assert len(batches) == 1
    assert len(batches[0]) == 100


def test_process_batches_with_callback():
    """Test at callback blir kalt etter hver batch."""
    data = [{'id': i} for i in range(1, 2501)]
    
    processed_batches = []
    callback_calls = []
    
    def process_fn(batch):
        processed_batches.append(len(batch))
    
    def on_complete(batch_num, total):
        callback_calls.append((batch_num, total))
    
    total = process_batches_with_callback(
        iter(data),
        batch_size=1000,
        process_fn=process_fn,
        on_batch_complete=on_complete
    )
    
    # Sjekk totalt antall
    assert total == 2500
    
    # Sjekk at 3 batches ble prosessert
    assert len(processed_batches) == 3
    assert processed_batches == [1000, 1000, 500]
    
    # Sjekk callback calls
    assert len(callback_calls) == 3
    assert callback_calls[0] == (1, 1000)
    assert callback_calls[1] == (2, 2000)
    assert callback_calls[2] == (3, 2500)


def test_engine_uses_batch_processing(monkeypatch, tmp_path):
    """Test at engine bruker batch processing korrekt."""
    import conduit_core.connectors.registry as registry
    
    original_source_map = registry._SOURCE_CONNECTOR_MAP
    original_dest_map = registry._DESTINATION_CONNECTOR_MAP
    
    # Create a destination instance that we can inspect
    batches_received = []
    
    class InspectableDestination(BaseDestination):
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
    
    def create_dest(config):
        return InspectableDestination()
    
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
            
            # Sjekk at destination mottok data i batches
            total_records = sum(len(batch) for batch in batches_received)
            assert total_records == 2500, f"Expected 2500 records, got {total_records}"
            
            # Skal ha mottatt 3 batches
            assert len(batches_received) == 3, f"Expected 3 batches, got {len(batches_received)}"
            assert len(batches_received[0]) == 1000, f"Batch 1: expected 1000, got {len(batches_received[0])}"
            assert len(batches_received[1]) == 1000, f"Batch 2: expected 1000, got {len(batches_received[1])}"
            assert len(batches_received[2]) == 500, f"Batch 3: expected 500, got {len(batches_received[2])}"
        
        finally:
            os.chdir(original_cwd)
    
    finally:
        registry._SOURCE_CONNECTOR_MAP = original_source_map
        registry._DESTINATION_CONNECTOR_MAP = original_dest_map


def test_batch_processing_memory_efficiency():
    """Test at batch processing ikke laster alt i minnet."""
    # Denne testen verifiserer at vi kun holder én batch om gangen
    
    max_memory_used = []
    
    class MemoryTrackingDestination(BaseDestination):
        def write(self, records):
            # Simuler at vi tracker hvor mye som er i minnet
            batch_size = len(list(records))
            max_memory_used.append(batch_size)
    
    dest = MemoryTrackingDestination()
    
    # Simuler 10,000 records
    data = [{'id': i} for i in range(1, 10001)]
    
    for batch in read_in_batches(iter(data), batch_size=1000):
        dest.write(batch)
    
    # Ingen enkelt batch skal være større enn 1000
    assert all(size <= 1000 for size in max_memory_used)
    assert max(max_memory_used) == 1000