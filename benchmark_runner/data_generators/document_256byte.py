"""
256-byte document generator for MongoDB benchmarks.

Generates random documents of a configurable size (default ~256 bytes)
with a fixed schema: ``_id``, ``timestamp``, ``category``, ``value``,
``counter``, ``expireAt``, ``uniqueNumber``, ``uniqueString``, and an
optional ``payload`` padding field.

This module is designed to be shared across multiple benchmark types
(insert, update, read, etc.) so that different benchmarks can use the
same document shape for consistent comparisons.
"""

import itertools
import random
import string
import uuid
from datetime import datetime, timedelta, timezone


# Process-wide atomic counter for generating unique numbers.
_unique_counter = itertools.count(1)

# Base document size estimate (bytes) before padding.  Accounts for the
# eight fixed fields (_id, timestamp, category, value, counter,
# expireAt, uniqueNumber, uniqueString) plus BSON overhead.
_BASE_DOC_SIZE = 245


def generate_document(size_bytes: int = 256) -> dict:
    """Generate a random document of approximately the given size.

    The document always contains ``_id``, ``timestamp``, ``category``,
    ``value``, ``counter``, ``expireAt``, ``uniqueNumber``, and
    ``uniqueString`` fields.  When *size_bytes* exceeds the base size
    (~245 bytes) a ``payload`` string field is added to pad the
    document to the target size.

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
        "expireAt": datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30)),
        "uniqueNumber": next(_unique_counter),
        "uniqueString": str(uuid.uuid4()),
    }

    # Pad with a string field to reach approximate target size
    padding_size = max(0, size_bytes - _BASE_DOC_SIZE)
    if padding_size > 0:
        doc["payload"] = "".join(
            random.choices(string.ascii_letters + string.digits, k=padding_size)
        )

    return doc
