"""Tests for benchmark_runner.benchmarks.insert_common — shared document generation."""

from benchmark_runner.benchmarks.insert.insert_common import generate_document


class TestGenerateDocument:
    """Unit tests for the generate_document helper."""

    def test_has_required_fields(self):
        doc = generate_document()
        assert "_id" in doc
        assert "timestamp" in doc
        assert "category" in doc
        assert "value" in doc
        assert "counter" in doc

    def test_category_in_expected_set(self):
        for _ in range(50):
            doc = generate_document()
            assert doc["category"] in {"A", "B", "C", "D", "E"}

    def test_default_size_adds_payload(self):
        doc = generate_document(256)
        assert "payload" in doc
        assert isinstance(doc["payload"], str)
        # Padding should be roughly 256 - 150 = 106 chars
        assert len(doc["payload"]) == 106

    def test_small_size_no_payload(self):
        doc = generate_document(50)
        # 50 < base size estimate (150), so no padding
        assert "payload" not in doc

    def test_large_size(self):
        doc = generate_document(2048)
        assert "payload" in doc
        assert len(doc["payload"]) == 2048 - 150

    def test_unique_ids_across_calls(self):
        ids = {generate_document()["_id"] for _ in range(100)}
        assert len(ids) == 100

    def test_counter_is_int(self):
        doc = generate_document()
        assert isinstance(doc["counter"], int)

    def test_value_is_float(self):
        doc = generate_document()
        assert isinstance(doc["value"], float)
