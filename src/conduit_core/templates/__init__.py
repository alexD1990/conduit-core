"""
Template system package for Conduit Core.

Provides:
- TEMPLATE_REGISTRY: metadata for built-in pipeline templates
- get_template: lookup helper
- load_template_yaml: load YAML definitions from bundled files
"""

from .registry import TEMPLATE_REGISTRY, get_template, load_template_yaml

__all__ = [
    "TEMPLATE_REGISTRY",
    "get_template",
    "load_template_yaml",
]
