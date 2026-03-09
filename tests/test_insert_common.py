"""Tests for document generation — canonical module and backward-compatible re-export."""

from datetime import datetime, timezone

from benchmark_runner.data_generators.document_256byte import _BASE_DOC_SIZE, generate_document


class TestGenerateDocument:
    """Unit tests for the generate_document helper."""

    def test_has_required_fields(self):
        doc = generate_document()
        for field in (
            "_id",
            "timestamp",
            "category",
            "value",
            "counter",
            "expireAt",
            "uniqueNumber",
            "uniqueString",
        ):
            assert field in doc

    def test_category_in_expected_set(self):
        for _ in range(50):
            doc = generate_document()
            assert doc["category"] in {"A", "B", "C", "D", "E"}

    def test_default_size_adds_payload(self):
        doc = generate_document(256)
        assert "payload" in doc
        assert isinstance(doc["payload"], str)
        assert len(doc["payload"]) == 256 - _BASE_DOC_SIZE

    def test_small_size_no_payload(self):
        doc = generate_document(50)
        # 50 < base size estimate, so no padding
        assert "payload" not in doc

    def test_large_size(self):
        doc = generate_document(2048)
        assert "payload" in doc
        assert len(doc["payload"]) == 2048 - _BASE_DOC_SIZE

    def test_unique_ids_across_calls(self):
        ids = {generate_document()["_id"] for _ in range(100)}
        assert len(ids) == 100

    def test_counter_is_int(self):
        doc = generate_document()
        assert isinstance(doc["counter"], int)

    def test_value_is_float(self):
        doc = generate_document()
        assert isinstance(doc["value"], float)

    def test_expire_at_is_future_utc_datetime(self):
        doc = generate_document()
        assert isinstance(doc["expireAt"], datetime)
        assert doc["expireAt"].tzinfo is not None
        assert doc["expireAt"] > datetime.now(timezone.utc)

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
