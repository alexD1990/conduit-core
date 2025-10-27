"""
Type coercion module for applying intelligent type casting to records.
"""
from typing import Any, Dict, List
from ..types import coerce_record, TypeCoercer
from ..logging_utils import ConduitLogger

logger = ConduitLogger("type_coercion")


def apply_type_coercion(
    records: List[Dict[str, Any]],
    inferred_schema: dict,
    destination_config: Any,
    error_log: Any,
    current_batch_offset: int
) -> List[Dict[str, Any]]:
    """
    Apply type coercion to a batch of records.
    
    Args:
        records: List of records to coerce
        inferred_schema: Inferred schema with column types
        destination_config: Destination configuration
        error_log: Error log to track failures
        current_batch_offset: Current batch offset for row numbers
    
    Returns:
        List of coerced records
    """
    if not destination_config.enable_type_coercion or not inferred_schema or not records:
        return records
    
    coerced_records = []
    for record in records:
        try:
            coerced = coerce_record(
                record,
                inferred_schema,
                strict_mode=destination_config.strict_type_coercion,
                null_values=destination_config.custom_null_values
            )
            
            # Apply custom type mappings if specified
            if destination_config.type_mappings:
                coercer = TypeCoercer(
                    strict_mode=destination_config.strict_type_coercion,
                    null_values=destination_config.custom_null_values
                )
                for col, target_type in destination_config.type_mappings.items():
                    if col in coerced:
                        coerced[col] = coercer.coerce(
                            coerced[col],
                            target_type,
                            column_name=col
                        )
            
            coerced_records.append(coerced)
        except Exception as e:
            if destination_config.strict_type_coercion:
                logger.error(f"Type coercion failed for record: {e}")
                raise
            else:
                logger.warning(f"Type coercion failed for record, skipping: {e}")
                error_log.add_error(record, e, row_number=current_batch_offset + len(coerced_records) + 1)
    
    logger.debug(f"Type coercion applied to {len(coerced_records)} records")
    return coerced_records