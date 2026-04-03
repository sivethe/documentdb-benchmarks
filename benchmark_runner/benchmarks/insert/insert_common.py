"""
Shared helpers for insert benchmarks.

Re-exports ``generate_document`` from the shared data-generators
package so that existing import paths continue to work.
"""

from benchmark_runner.data_generators.document_standard import generate_document

__all__ = ["generate_document"]
