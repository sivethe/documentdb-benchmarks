"""Tests for document generation — canonical module, registry, and backward-compatible re-exports."""

import itertools
from datetime import datetime

from benchmark_runner.data_generators.document_standard import (
    generate_document,
)
from benchmark_runner.data_generators import get_generator
import benchmark_runner.data_generators.document_standard as ds


class TestGenerateDocument:
    """Unit tests for the generate_document helper."""

    def test_has_required_core_fields(self):
        doc = generate_document()
        for field in (
            "_id",
            "uniqueNumber",
            "uniqueString",
            "category",
            "status",
            "value",
            "counter",
            "createdAt",
            "expireAt",
            "isActive",
        ):
            assert field in doc, f"Missing core field: {field}"

    def test_category_in_expected_set(self):
        for _ in range(50):
            doc = generate_document()
            assert doc["category"] in {"A", "B", "C", "D", "E"}

    def test_status_in_expected_set(self):
        for _ in range(50):
            doc = generate_document()
            assert doc["status"] in {"active", "inactive", "pending"}

    def test_is_active_is_bool(self):
        doc = generate_document()
        assert isinstance(doc["isActive"], bool)

    def test_default_size_small_payload(self):
        doc = generate_document(256)
        # At 256 bytes, core (237) + payload overhead (14) + a few chars
        assert "payload" in doc
        assert isinstance(doc["payload"], str)

    def test_small_size_no_extras(self):
        doc = generate_document(50)
        # 50 < core size, so no tier fields and no payload
        assert "payload" not in doc
        assert "tags" not in doc
        assert "metadata" not in doc

    def test_medium_size_has_tags_and_metadata(self):
        doc = generate_document(512)
        assert "tags" in doc
        assert isinstance(doc["tags"], list)
        assert all(isinstance(t, str) for t in doc["tags"])
        assert "metadata" in doc
        assert isinstance(doc["metadata"], dict)
        assert "source" in doc["metadata"]

    def test_large_size_has_all_tiers(self):
        doc = generate_document(4096)
        assert "tags" in doc
        assert "metadata" in doc
        assert "profile" in doc
        assert isinstance(doc["profile"], dict)
        assert "address" in doc["profile"]
        assert isinstance(doc["profile"]["address"], dict)
        assert "events" in doc
        assert isinstance(doc["events"], list)
        assert len(doc["events"]) >= 2
        assert "items" in doc
        assert isinstance(doc["items"], list)
        assert len(doc["items"]) >= 2
        assert "payload" in doc

    def test_event_structure(self):
        doc = generate_document(4096)
        event = doc["events"][0]
        assert "type" in event
        assert "ts" in event
        assert isinstance(event["ts"], datetime)
        assert "amount" in event
        assert isinstance(event["amount"], float)
        assert "detail" in event

    def test_item_structure(self):
        doc = generate_document(4096)
        item = doc["items"][0]
        assert "sku" in item
        assert "qty" in item
        assert isinstance(item["qty"], int)
        assert "price" in item
        assert isinstance(item["price"], float)
        assert "description" in item

    def test_profile_address_structure(self):
        doc = generate_document(4096)
        addr = doc["profile"]["address"]
        for field in ("street", "city", "state", "zipCode", "country"):
            assert field in addr, f"Missing address field: {field}"

    def test_unique_ids_across_calls(self):
        ids = {generate_document()["_id"] for _ in range(100)}
        assert len(ids) == 100

    def test_counter_is_int(self):
        doc = generate_document()
        assert isinstance(doc["counter"], int)

    def test_value_is_float(self):
        doc = generate_document()
        assert isinstance(doc["value"], float)

    def test_created_at_is_utc_datetime(self):
        doc = generate_document()
        assert isinstance(doc["createdAt"], datetime)
        assert doc["createdAt"].tzinfo is not None

    def test_expire_at_is_utc_datetime(self):
        doc = generate_document()
        assert isinstance(doc["expireAt"], datetime)
        assert doc["expireAt"].tzinfo is not None

    def test_unique_number_is_int_and_unique(self):
        docs = [generate_document() for _ in range(100)]
        numbers = [d["uniqueNumber"] for d in docs]
        assert all(isinstance(n, int) for n in numbers)
        assert len(set(numbers)) == 100

    def test_unique_string_is_str_and_unique(self):
        docs = [generate_document() for _ in range(100)]
        strings = [d["uniqueString"] for d in docs]
        assert all(isinstance(s, str) for s in strings)
        assert len(set(strings)) == 100

    def test_determinism(self):
        """Same uniqueNumber sequence produces identical documents."""
        ds._unique_counter = itertools.count(1)
        doc_a = generate_document(1024)
        ds._unique_counter = itertools.count(1)
        doc_b = generate_document(1024)
        assert doc_a == doc_b

    def test_arrays_scale_with_size(self):
        doc_small = generate_document(2048)
        doc_large = generate_document(16384)
        assert len(doc_large.get("events", [])) > len(doc_small.get("events", []))


class TestInsertCommonReExport:
    """Verify that the old import path still works."""

    def test_backward_compatible_import(self):
        from benchmark_runner.benchmarks.insert.insert_common import (
            generate_document as gen_compat,
        )

        doc = gen_compat(256)
        assert "_id" in doc
        assert "expireAt" in doc
        assert "uniqueNumber" in doc
        assert "uniqueString" in doc


class TestGetGenerator:
    """Verify the data_generators registry / factory."""

    def test_returns_standard_generator(self):
        gen = get_generator("standard")
        doc = gen(256)
        assert "_id" in doc
        assert "category" in doc

    def test_default_is_standard(self):
        gen = get_generator()
        doc = gen(512)
        assert "_id" in doc

    def test_unknown_generator_raises(self):
        import pytest

        with pytest.raises(ValueError, match="Unknown data generator"):
            get_generator("nonexistent")
