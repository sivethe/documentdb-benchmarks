"""
Shared helpers for TTL (Time-To-Live) benchmarks.

Provides document generation, collection seeding, and index management
utilities used by all TTL benchmark variants (deletion monitoring,
query impact, insert-during-delete).

The TTL benchmarks measure how quickly MongoDB-compatible databases
delete expired documents via TTL indexes, and optionally measure the
performance impact on concurrent operations.
"""

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pymongo

from benchmark_runner.data_generators.document_256byte import generate_document

logger = logging.getLogger(__name__)

# Batch size for bulk seeding operations.
_SEED_BATCH_SIZE = 5000


def generate_ttl_document(
    expired: bool,
    size_bytes: int = 256,
    expire_after_minutes: int = 0,
) -> dict:
    """Generate a document with a TTL-appropriate ``expireAt`` value.

    Calls ``document_256byte.generate_document(size_bytes)`` for the base
    document, then overrides the ``expireAt`` field:

    - **Expired docs**: ``expireAt`` is set in the past (1 hour ago) so the
      TTL monitor deletes them once a TTL index exists.
    - **Alive docs**: ``expireAt`` is set far in the future (30 days).
    - **Expire-after delay**: When *expire_after_minutes* > 0, expired docs
      get ``expireAt = now + expire_after_minutes`` so they transition from
      alive to expired after the specified delay.

    Args:
        expired: Whether the document should be expired.
        size_bytes: Approximate target document size in bytes.
        expire_after_minutes: Minutes from now until expiration (0 = already expired).

    Returns:
        A dict suitable for inserting into a MongoDB collection.
    """
    doc = generate_document(size_bytes)

    now = datetime.now(timezone.utc)
    if expired:
        if expire_after_minutes > 0:
            doc["expireAt"] = now + timedelta(minutes=expire_after_minutes)
        else:
            doc["expireAt"] = now - timedelta(hours=1)
    else:
        doc["expireAt"] = now + timedelta(days=30)

    return doc


def seed_ttl_collection(
    collection: Any,
    num_docs: int,
    expired_pct: float = 10.0,
    document_size: int = 256,
    expire_after_minutes: int = 0,
) -> Dict[str, int]:
    """Seed a collection with a mix of alive and expired documents.

    Documents are inserted in batches using ``insert_many(ordered=False)``
    for performance.

    Args:
        collection: A pymongo Collection to seed.
        num_docs: Total number of documents to insert.
        expired_pct: Percentage of documents that should be expired (0-100).
        document_size: Approximate size of each document in bytes.
        expire_after_minutes: Minutes from now until expired docs expire.

    Returns:
        Dict with ``expired_count`` and ``alive_count`` tallies.
    """
    num_expired = int(num_docs * expired_pct / 100.0)
    num_alive = num_docs - num_expired

    logger.info(
        "Seeding %s with %d docs (%d expired, %d alive, document_size=%d)",
        collection.name,
        num_docs,
        num_expired,
        num_alive,
        document_size,
    )

    total_inserted = 0
    expired_inserted = 0

    # Insert in batches
    total_batches = math.ceil(num_docs / _SEED_BATCH_SIZE)
    for batch_idx in range(total_batches):
        batch_start = batch_idx * _SEED_BATCH_SIZE
        batch_end = min(batch_start + _SEED_BATCH_SIZE, num_docs)
        batch_size = batch_end - batch_start

        batch = []
        for i in range(batch_size):
            doc_index = batch_start + i
            is_expired = doc_index < num_expired
            doc = generate_ttl_document(
                expired=is_expired,
                size_bytes=document_size,
                expire_after_minutes=expire_after_minutes,
            )
            batch.append(doc)
            if is_expired:
                expired_inserted += 1

        collection.insert_many(batch, ordered=False)
        total_inserted += len(batch)

        if (batch_idx + 1) % 20 == 0 or batch_idx == total_batches - 1:
            logger.info(
                "  Seeded %d / %d docs (%.0f%%)",
                total_inserted,
                num_docs,
                100.0 * total_inserted / num_docs,
            )

    logger.info(
        "Seeding complete: %d total (%d expired, %d alive)",
        total_inserted,
        expired_inserted,
        total_inserted - expired_inserted,
    )
    return {"expired_count": expired_inserted, "alive_count": total_inserted - expired_inserted}


def create_extra_indexes(
    collection: Any,
    database_engine: str = "",
) -> List[str]:
    """Create the 4 heterogeneous extra indexes on a TTL collection.

    The indexes match the insert benchmark index types:
    1. Simple (single-path) ascending index on ``timestamp``
    2. Composite ascending index on ``(category, value)``
    3. Unique ascending index on ``uniqueString``
    4. Wildcard index on ``$**``

    Args:
        collection: A pymongo Collection.
        database_engine: Engine name for engine-specific options.

    Returns:
        List of created index names.
    """
    kwargs: Dict[str, Any] = {}
    if database_engine == "azure_documentdb":
        kwargs["storageEngine"] = {"enableOrderedIndex": True}

    created = []

    # 1. Simple (single-path) index on timestamp
    name = collection.create_index(
        [("timestamp", pymongo.ASCENDING)],
        name="idx_timestamp_asc",
        **kwargs,
    )
    created.append(name)
    logger.info("Created simple index '%s' on %s", name, collection.name)

    # 2. Composite index on (category, value)
    name = collection.create_index(
        [("category", pymongo.ASCENDING), ("value", pymongo.ASCENDING)],
        name="idx_category_value_asc",
        **kwargs,
    )
    created.append(name)
    logger.info("Created composite index '%s' on %s", name, collection.name)

    # 3. Unique index on uniqueString
    name = collection.create_index(
        [("uniqueString", pymongo.ASCENDING)],
        name="idx_uniqueString_unique",
        unique=True,
        **kwargs,
    )
    created.append(name)
    logger.info("Created unique index '%s' on %s", name, collection.name)

    # 4. Wildcard index
    wildcard_kwargs = dict(kwargs)
    # Wildcard indexes may not support storageEngine on all engines
    name = collection.create_index(
        [("$**", 1)],
        name="idx_wildcard",
    )
    created.append(name)
    logger.info("Created wildcard index '%s' on %s", name, collection.name)

    return created


def create_ttl_index(
    collection: Any,
    database_engine: str = "",
) -> str:
    """Create a TTL index on the ``expireAt`` field.

    The index uses ``expireAfterSeconds=0`` so documents are eligible for
    deletion as soon as their ``expireAt`` timestamp is in the past.

    Args:
        collection: A pymongo Collection.
        database_engine: Engine name for engine-specific options.

    Returns:
        The created index name.
    """
    kwargs: Dict[str, Any] = {}
    if database_engine == "azure_documentdb":
        kwargs["storageEngine"] = {"enableOrderedIndex": True}

    name = collection.create_index(
        [("expireAt", pymongo.ASCENDING)],
        name="idx_expireAt_ttl",
        expireAfterSeconds=0,
        **kwargs,
    )
    logger.info("Created TTL index '%s' on %s (expireAfterSeconds=0)", name, collection.name)
    return name


def drop_ttl_index(collection: Any) -> None:
    """Drop the TTL index on the ``expireAt`` field if it exists.

    Args:
        collection: A pymongo Collection.
    """
    try:
        collection.drop_index("idx_expireAt_ttl")
        logger.info("Dropped TTL index 'idx_expireAt_ttl' on %s", collection.name)
    except Exception as exc:
        logger.debug("Could not drop TTL index (may not exist): %s", exc)
