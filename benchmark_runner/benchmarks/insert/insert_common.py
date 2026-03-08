"""
Shared helpers for insert benchmarks.

Provides document generation used by all insert benchmark variants.
"""

import random
import string
import uuid


def generate_document(size_bytes: int = 256) -> dict:
    """Generate a random document of approximately the given size.

    The document always contains ``_id``, ``timestamp``, ``category``,
    ``value``, and ``counter`` fields.  When *size_bytes* exceeds the
    base size (~150 bytes) a ``payload`` string field is added to pad
    the document to the target size.

    Args:
        size_bytes: Approximate target size in bytes.

    Returns:
        A dict suitable for inserting into a MongoDB collection.
    """
    doc = {
        "_id": str(uuid.uuid4()),
        "timestamp": random.random() * 1e12,
        "category": random.choice(["A", "B", "C", "D", "E"]),
        "value": random.uniform(0, 1000),
        "counter": random.randint(0, 1_000_000),
    }

    # Pad with a string field to reach approximate target size
    # Base doc is ~150 bytes; add padding
    current_size = 150  # rough estimate
    padding_size = max(0, size_bytes - current_size)
    if padding_size > 0:
        doc["payload"] = "".join(
            random.choices(string.ascii_letters + string.digits, k=padding_size)
        )

    return doc
