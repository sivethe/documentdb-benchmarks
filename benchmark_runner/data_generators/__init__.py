"""
benchmark_runner.data_generators — Shared document generators for benchmarks.

Each generator module exposes a ``generate_document(size_bytes: int) -> dict``
function.  Benchmarks select a generator by setting the ``data_generator``
workload parameter in their YAML config (default: ``"standard"``).

Use :func:`get_generator` to obtain the ``generate_document`` callable for a
given generator name at runtime.
"""

import importlib
from typing import Callable, Dict

# Registry mapping short names to module paths within this package.
_GENERATORS: Dict[str, str] = {
    "standard": "benchmark_runner.data_generators.document_standard",
}


def get_generator(name: str = "standard") -> Callable[[int], dict]:
    """Return the ``generate_document`` function for the named generator.

    Args:
        name: Generator name matching the ``data_generator`` workload
              parameter in YAML config (default: ``"standard"``).

    Returns:
        A ``generate_document(size_bytes: int) -> dict`` callable.

    Raises:
        ValueError: If no generator is registered under *name*.
    """
    module_path = _GENERATORS.get(name)
    if module_path is None:
        available = ", ".join(sorted(_GENERATORS))
        raise ValueError(f"Unknown data generator '{name}'. Available generators: {available}")
    mod = importlib.import_module(module_path)
    return mod.generate_document
