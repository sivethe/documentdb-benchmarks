"""
Standard document generator for MongoDB benchmarks.

Generates deterministic documents of a configurable size (default ~256 bytes)
with a tiered schema.  Every document contains core scalar fields (``_id``,
``uniqueNumber``, ``uniqueString``, ``category``, ``status``, ``value``,
``counter``, ``createdAt``, ``expireAt``, ``isActive``).  As the requested
*size_bytes* increases, additional structured tiers are added progressively:

1. ``tags`` — array of scalar strings (multikey index, ``$in``, ``$addToSet``)
2. ``metadata`` — embedded document (dot-notation queries, nested ``$set``)
3. ``profile`` — deeply nested document with ``address`` sub-document
4. ``events`` — scalable array of embedded documents (``$push``, ``$unwind``)
5. ``items`` — second scalable array of embedded documents
6. ``payload`` — string padding for fine-tuning to exact target size

Documents are **deterministic**: the same ``uniqueNumber`` always produces
the same document for a given *size_bytes*, making benchmarks reproducible.

This module is designed to be shared across multiple benchmark types
(insert, update, read, etc.) so that different benchmarks can use the
same document shape for consistent comparisons.

Schema
------

**Core fields (~237 bytes BSON) — always present:**

=============  ==========================  ==========================================
Field          Type                        Purpose
=============  ==========================  ==========================================
``_id``        string (UUID)               Primary key
``uniqueNumber`` int (sequential)          Exact-match lookups, RNG seed
``uniqueString`` string (UUID)             Secondary unique identifier
``category``   string (5 values)           Low-cardinality grouping
``status``     string (3 values)           Second categorical dimension
``value``      float                       Numeric aggregation ($sum, $avg)
``counter``    int                         $inc target
``createdAt``  datetime (UTC)              Date range queries, sorting
``expireAt``   datetime (UTC)              TTL index testing
``isActive``   boolean (~70 % true)        Boolean predicates, partial indexes
=============  ==========================  ==========================================

**Tiered fields (added progressively as size budget allows):**

====  =============  ======================================  =========  ==========================================
Tier  Field          Type                                    ~Size      Exercises
====  =============  ======================================  =========  ==========================================
1     ``tags``       array of 3+ strings                     ~57 B      Multikey indexes, ``$in``, ``$addToSet``
2     ``metadata``   embedded doc {source, version,          ~89 B      Dot-notation queries, nested ``$set``
                     region, priority}
3     ``profile``    nested doc {firstName, lastName,         ~217 B     Multi-level dot notation
                     email, phone, address: {street,
                     city, state, zipCode, country}}
4     ``events``     array of {type, ts, amount, detail}     ~77 B ea.  ``$push``, ``$unwind``, ``$elemMatch``
5     ``items``      array of {sku, qty, price, description} ~93 B ea.  Second array dimension, ``$addToSet``
6     ``payload``    string                                  remaining  Fine-tune to exact target size
====  =============  ======================================  =========  ==========================================

**Size map (approximate BSON-encoded sizes):**

=========  ====  ====  ========  =======  ======  =====  =======
Target     Core  tags  metadata  profile  events  items  payload
=========  ====  ====  ========  =======  ======  =====  =======
256 B      ✓     —     —         —        —       —      ~5 B
512 B      ✓     3     ✓         —        —       —      ~115 B
1 KB       ✓     4     ✓         ✓        3       —      ~164 B
4 KB       ✓     10    ✓         ✓        ~26     ~11    ~366 B
16 KB      ✓     20    ✓         ✓        ~121    ~50    ~1.5 KB
128 KB     ✓     20    ✓         ✓        ~1014   ~420   ~13 KB
1 MB       ✓     20    ✓         ✓        ~8164   ~3379  ~105 KB
=========  ====  ====  ========  =======  ======  =====  =======

**Determinism:** each document is fully reproducible — ``uniqueNumber``
seeds a local ``random.Random()`` instance, dates derive from a fixed
epoch + RNG offset (no ``datetime.now()``), UUIDs from
``uuid.UUID(int=rng.getrandbits(128))``.
"""

import itertools
import random
import string
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

# ---------------------------------------------------------------------------
# Process-wide atomic counter
# ---------------------------------------------------------------------------
_unique_counter = itertools.count(1)

# ---------------------------------------------------------------------------
# Size constants (empirically measured BSON sizes)
# ---------------------------------------------------------------------------

# Core document size estimate (bytes).  Accounts for the ten fixed scalar
# fields plus BSON overhead.  Measured with bson.encode() on a representative
# core-only document.
_CORE_SIZE = 237

# Approximate BSON overhead per tier (measured empirically).
_TAGS_BASE_SIZE = 57  # 3 tags
_METADATA_SIZE = 89  # embedded doc with 4 fields
_PROFILE_SIZE = 217  # nested doc with address sub-doc
_EVENT_SIZE = 77  # single event embedded doc
_ITEM_SIZE = 93  # single item embedded doc

# ---------------------------------------------------------------------------
# Static pools for deterministic choices
# ---------------------------------------------------------------------------
_CATEGORIES = ["A", "B", "C", "D", "E"]
_STATUSES = ["active", "inactive", "pending"]
_TAG_POOL = [
    "electronics",
    "urgent",
    "featured",
    "sale",
    "new",
    "premium",
    "clearance",
    "trending",
    "limited",
    "exclusive",
    "wholesale",
    "imported",
    "handmade",
    "organic",
    "certified",
    "refurbished",
    "seasonal",
    "popular",
    "recommended",
    "verified",
]
_SOURCES = ["web", "mobile", "api", "batch"]
_VERSIONS = ["1.0", "1.1", "2.0", "2.1", "3.0"]
_REGIONS = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
    "sa-east-1",
    "ca-central-1",
]
_FIRST_NAMES = [
    "Alice",
    "Bob",
    "Carol",
    "David",
    "Eve",
    "Frank",
    "Grace",
    "Hank",
    "Ivy",
    "Jack",
]
_LAST_NAMES = [
    "Smith",
    "Jones",
    "Brown",
    "Davis",
    "Wilson",
    "Clark",
    "Lewis",
    "Walker",
    "Hall",
    "Young",
]
_CITIES = [
    "Seattle",
    "London",
    "Tokyo",
    "Berlin",
    "Sydney",
    "Toronto",
    "Mumbai",
    "São Paulo",
    "Cairo",
    "Singapore",
]
_STATES = [
    "WA",
    "CA",
    "NY",
    "TX",
    "FL",
    "IL",
    "PA",
    "OH",
    "GA",
    "NC",
]
_COUNTRIES = [
    "US",
    "UK",
    "JP",
    "DE",
    "AU",
    "CA",
    "IN",
    "BR",
    "EG",
    "SG",
]
_EVENT_TYPES = ["click", "view", "purchase", "refund", "login", "logout", "search", "share"]

# Fixed epoch for deterministic date generation (no datetime.now()).
_EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)

# Characters used for deterministic string padding.
_PAD_CHARS = string.ascii_letters + string.digits


# ---------------------------------------------------------------------------
# Tier helpers
# ---------------------------------------------------------------------------


def _make_tags(rng: random.Random, count: int) -> List[str]:
    """Return *count* unique tags chosen deterministically from the pool."""
    return rng.sample(_TAG_POOL, min(count, len(_TAG_POOL)))


def _make_metadata(rng: random.Random) -> dict:
    """Return a metadata embedded document."""
    return {
        "source": rng.choice(_SOURCES),
        "version": rng.choice(_VERSIONS),
        "region": rng.choice(_REGIONS),
        "priority": rng.randint(1, 5),
    }


def _make_profile(rng: random.Random) -> dict:
    """Return a profile document with a nested address sub-document."""
    first = rng.choice(_FIRST_NAMES)
    last = rng.choice(_LAST_NAMES)
    return {
        "firstName": first,
        "lastName": last,
        "email": f"{first.lower()}.{last.lower()}@example.com",
        "phone": f"+1-{rng.randint(200, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}",
        "address": {
            "street": f"{rng.randint(1, 9999)} {rng.choice(['Main', 'Oak', 'Pine', 'Elm', 'Cedar'])} St",
            "city": rng.choice(_CITIES),
            "state": rng.choice(_STATES),
            "zipCode": f"{rng.randint(10000, 99999)}",
            "country": rng.choice(_COUNTRIES),
        },
    }


def _make_event(rng: random.Random) -> dict:
    """Return a single event embedded document."""
    return {
        "type": rng.choice(_EVENT_TYPES),
        "ts": _EPOCH + timedelta(seconds=rng.randint(0, 365 * 24 * 3600)),
        "amount": round(rng.uniform(0, 500), 2),
        "detail": "".join(rng.choices(_PAD_CHARS, k=12)),
    }


def _make_item(rng: random.Random) -> dict:
    """Return a single item embedded document."""
    return {
        "sku": f"SKU-{rng.randint(10000, 99999)}",
        "qty": rng.randint(1, 100),
        "price": round(rng.uniform(1, 1000), 2),
        "description": "".join(rng.choices(_PAD_CHARS, k=24)),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_document(size_bytes: int = 256) -> dict:
    """Generate a deterministic document of approximately the given size.

    The document always contains ten core scalar fields.  As *size_bytes*
    increases, structured tiers are added progressively (tags, metadata,
    profile, events array, items array) before a final string ``payload``
    pads to the exact target.  Two calls that produce the same
    ``uniqueNumber`` with the same *size_bytes* will return identical
    documents.

    Args:
        size_bytes: Approximate target size in bytes (default 256).

    Returns:
        A dict suitable for inserting into a MongoDB collection.
    """
    unique_num = next(_unique_counter)
    rng = random.Random(unique_num)

    # --- Core fields (always present) ---
    doc: dict = {
        "_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "uniqueNumber": unique_num,
        "uniqueString": str(uuid.UUID(int=rng.getrandbits(128))),
        "category": rng.choice(_CATEGORIES),
        "status": rng.choice(_STATUSES),
        "value": round(rng.uniform(0, 1000), 4),
        "counter": rng.randint(0, 1_000_000),
        "createdAt": _EPOCH + timedelta(seconds=rng.randint(0, 365 * 24 * 3600)),
        "expireAt": _EPOCH
        + timedelta(seconds=rng.randint(0, 365 * 24 * 3600) + rng.randint(1, 30) * 86400),
        "isActive": rng.random() < 0.7,
    }

    budget = size_bytes
    used = _CORE_SIZE
    remaining = budget - used

    # --- Tier 1: tags (array of scalars) ---
    if remaining >= _TAGS_BASE_SIZE:
        tag_count = min(3 + remaining // 500, len(_TAG_POOL))
        doc["tags"] = _make_tags(rng, tag_count)
        extra_tag_bytes = max(0, tag_count - 3) * 15
        used += _TAGS_BASE_SIZE + extra_tag_bytes
        remaining = budget - used

    # --- Tier 2: metadata (embedded document) ---
    if remaining >= _METADATA_SIZE:
        doc["metadata"] = _make_metadata(rng)
        used += _METADATA_SIZE
        remaining = budget - used

    # --- Tier 3: profile (deeply nested document) ---
    if remaining >= _PROFILE_SIZE:
        doc["profile"] = _make_profile(rng)
        used += _PROFILE_SIZE
        remaining = budget - used

    # --- Tier 4: events (scalable array of embedded documents) ---
    if remaining >= 2 * _EVENT_SIZE:
        # events get ~60% of remaining budget, items get the rest
        events_budget = int(remaining * 0.6)
        event_count = max(2, events_budget // _EVENT_SIZE)
        doc["events"] = [_make_event(rng) for _ in range(event_count)]
        used += event_count * _EVENT_SIZE
        remaining = budget - used

    # --- Tier 5: items (second scalable array) ---
    if remaining >= 2 * _ITEM_SIZE:
        items_budget = int(remaining * 0.75)
        item_count = max(2, items_budget // _ITEM_SIZE)
        doc["items"] = [_make_item(rng) for _ in range(item_count)]
        used += item_count * _ITEM_SIZE
        remaining = budget - used

    # --- Tier 6: payload (string padding for fine-tuning) ---
    # Subtract BSON string overhead (type byte + key name + length prefix + null).
    _PAYLOAD_BSON_OVERHEAD = 14
    if remaining > _PAYLOAD_BSON_OVERHEAD:
        doc["payload"] = "".join(rng.choices(_PAD_CHARS, k=remaining - _PAYLOAD_BSON_OVERHEAD))

    return doc
