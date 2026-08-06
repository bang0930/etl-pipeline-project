from importlib import import_module

from quality.exceptions import DataQualityError, PaginationConsistencyError


__all__ = [
    "DataQualityError",
    "PaginationConsistencyError",
    "validate_raw_batch",
    "validate_mart_rankings",
    "validate_transformed_items",
    "validate_staging_load",
]

_VALIDATOR_EXPORTS = {
    "validate_raw_batch",
    "validate_mart_rankings",
    "validate_transformed_items",
    "validate_staging_load",
}


def __getattr__(name):
    """순환 import 없이 기존 quality validator 공개 API를 유지한다."""
    if name in _VALIDATOR_EXPORTS:
        validators = import_module("quality.validators")
        return getattr(validators, name)

    raise AttributeError(f"module 'quality' has no attribute {name!r}")
