"""Tests for benchmark_runner.config — YAML loading and inheritance."""

import textwrap

import pytest

from benchmark_runner.config import _deep_merge, build_config, load_config


# ---------- _deep_merge ----------


class TestDeepMerge:
    def test_flat_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 99, "c": 3}
        assert _deep_merge(base, override) == {"a": 1, "b": 99, "c": 3}

    def test_nested_dict_merge(self):
        base = {"wp": {"x": 1, "y": 2}, "other": "keep"}
        override = {"wp": {"y": 99, "z": 3}}
        result = _deep_merge(base, override)
        assert result == {"wp": {"x": 1, "y": 99, "z": 3}, "other": "keep"}

    def test_override_replaces_non_dict(self):
        base = {"a": [1, 2]}
        override = {"a": [3]}
        assert _deep_merge(base, override) == {"a": [3]}

    def test_does_not_mutate_inputs(self):
        base = {"wp": {"x": 1}}
        override = {"wp": {"y": 2}}
        result = _deep_merge(base, override)
        result["wp"]["x"] = 999
        assert base["wp"]["x"] == 1
        assert override["wp"].get("x") is None


# ---------- load_config with inherits ----------


class TestLoadConfigInheritance:
    def test_simple_inherit(self, tmp_path):
        parent = tmp_path / "parent.yaml"
        parent.write_text(
            textwrap.dedent("""\
            benchmark_name: parent_bench
            benchmark_module: benchmarks.insert_benchmark
            users: 10
            workload_params:
              document_size: 256
              sharded: false
        """)
        )

        child = tmp_path / "child.yaml"
        child.write_text(
            textwrap.dedent("""\
            inherits: parent.yaml
            benchmark_name: child_bench
            workload_params:
              sharded: true
        """)
        )

        result = load_config(str(child))

        assert result["benchmark_name"] == "child_bench"
        assert result["benchmark_module"] == "benchmarks.insert_benchmark"
        assert result["users"] == 10
        # workload_params deep-merged: document_size from parent, sharded overridden
        assert result["workload_params"]["document_size"] == 256
        assert result["workload_params"]["sharded"] is True

    def test_chained_inheritance(self, tmp_path):
        grandparent = tmp_path / "grandparent.yaml"
        grandparent.write_text(
            textwrap.dedent("""\
            benchmark_name: gp
            users: 5
            workload_params:
              a: 1
              b: 2
        """)
        )

        parent = tmp_path / "parent.yaml"
        parent.write_text(
            textwrap.dedent("""\
            inherits: grandparent.yaml
            benchmark_name: parent
            workload_params:
              b: 20
              c: 30
        """)
        )

        child = tmp_path / "child.yaml"
        child.write_text(
            textwrap.dedent("""\
            inherits: parent.yaml
            benchmark_name: child
            workload_params:
              c: 300
        """)
        )

        result = load_config(str(child))

        assert result["benchmark_name"] == "child"
        assert result["users"] == 5
        assert result["workload_params"] == {"a": 1, "b": 20, "c": 300}

    def test_circular_inheritance_raises(self, tmp_path):
        a = tmp_path / "a.yaml"
        b = tmp_path / "b.yaml"
        a.write_text("inherits: b.yaml\nname: a\n")
        b.write_text("inherits: a.yaml\nname: b\n")

        with pytest.raises(ValueError, match="Circular config inheritance"):
            load_config(str(a))

    def test_no_inherits_key(self, tmp_path):
        cfg = tmp_path / "standalone.yaml"
        cfg.write_text("benchmark_name: solo\nusers: 42\n")

        result = load_config(str(cfg))
        assert result == {"benchmark_name": "solo", "users": 42}

    def test_inherits_key_not_in_result(self, tmp_path):
        parent = tmp_path / "parent.yaml"
        parent.write_text("benchmark_name: p\n")

        child = tmp_path / "child.yaml"
        child.write_text("inherits: parent.yaml\nbenchmark_name: c\n")

        result = load_config(str(child))
        assert "inherits" not in result

    def test_subdirectory_inherits(self, tmp_path):
        """Child in a subdirectory can reference parent via relative path."""
        parent = tmp_path / "parent.yaml"
        parent.write_text("benchmark_name: p\nusers: 7\n")

        subdir = tmp_path / "sub"
        subdir.mkdir()
        child = subdir / "child.yaml"
        child.write_text("inherits: ../parent.yaml\nbenchmark_name: c\n")

        result = load_config(str(child))
        assert result["benchmark_name"] == "c"
        assert result["users"] == 7


# ---------- integration: load_config -> build_config ----------


class TestBuildConfigWithInheritance:
    def test_inherited_config_builds(self, tmp_path):
        parent = tmp_path / "parent.yaml"
        parent.write_text(
            textwrap.dedent("""\
            benchmark_name: parent_bench
            benchmark_module: benchmarks.insert_benchmark
            users: 10
            run_time: "120s"
            workload_params:
              document_size: 256
              sharded: false
        """)
        )

        child = tmp_path / "child.yaml"
        child.write_text(
            textwrap.dedent("""\
            inherits: parent.yaml
            benchmark_name: sharded_bench
            workload_params:
              sharded: true
        """)
        )

        config_dict = load_config(str(child))
        config = build_config(config_dict=config_dict)

        assert config.benchmark_name == "sharded_bench"
        assert config.benchmark_module == "benchmarks.insert_benchmark"
        assert config.users == 10
        assert config.run_time == "120s"
        assert config.workload_params["document_size"] == 256
        assert config.workload_params["sharded"] is True
