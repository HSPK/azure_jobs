import pytest
from pathlib import Path

import yaml

from azure_jobs.core.conf import read_conf, ConfigError


def write_yaml(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(data))


class TestReadConfBasic:
    def test_simple_config(self, aj_home):
        fp = aj_home / "simple.yaml"
        write_yaml(fp, {"config": {"key": "value"}})
        assert read_conf(fp) == {"key": "value"}

    def test_config_without_base(self, aj_home):
        fp = aj_home / "test.yaml"
        write_yaml(fp, {"config": {"a": 1, "b": 2}})
        assert read_conf(fp) == {"a": 1, "b": 2}

    def test_empty_file_returns_empty_dict(self, aj_home):
        fp = aj_home / "empty.yaml"
        fp.write_text("")
        assert read_conf(fp) == {}

    def test_file_not_found(self, aj_home):
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            read_conf(aj_home / "nonexistent.yaml")

    def test_no_config_key_returns_empty(self, aj_home):
        fp = aj_home / "noconfig.yaml"
        write_yaml(fp, {"base": None})
        assert read_conf(fp) == {}


class TestReadConfInheritance:
    def test_single_base(self, aj_home):
        base_fp = aj_home / "base.yaml"
        write_yaml(base_fp, {"config": {"from_base": True, "shared": "base"}})

        child_fp = aj_home / "child.yaml"
        write_yaml(child_fp, {
            "base": "base",
            "config": {"from_child": True, "shared": "child"},
        })
        result = read_conf(child_fp)
        assert result == {"from_base": True, "from_child": True, "shared": "child"}

    def test_multiple_bases(self, aj_home):
        write_yaml(aj_home / "a.yaml", {"config": {"source": "a", "only_a": 1}})
        write_yaml(aj_home / "b.yaml", {"config": {"source": "b", "only_b": 2}})

        child_fp = aj_home / "child.yaml"
        write_yaml(child_fp, {
            "base": ["a", "b"],
            "config": {"source": "child"},
        })
        result = read_conf(child_fp)
        assert result["source"] == "child"
        assert result["only_a"] == 1
        assert result["only_b"] == 2

    def test_chained_inheritance(self, aj_home):
        write_yaml(aj_home / "grandparent.yaml", {"config": {"level": "grandparent", "gp_only": True}})
        write_yaml(aj_home / "parent.yaml", {
            "base": "grandparent",
            "config": {"level": "parent", "parent_only": True},
        })
        write_yaml(aj_home / "child.yaml", {
            "base": "parent",
            "config": {"level": "child"},
        })
        result = read_conf(aj_home / "child.yaml")
        assert result == {"level": "child", "gp_only": True, "parent_only": True}

    def test_dotted_base_resolves_to_aj_home(self, aj_home):
        subdir = aj_home / "envs"
        subdir.mkdir(parents=True)
        write_yaml(subdir / "gpu.yaml", {"config": {"gpu": True}})

        child_fp = aj_home / "child.yaml"
        write_yaml(child_fp, {
            "base": "envs.gpu",
            "config": {"name": "my_job"},
        })
        result = read_conf(child_fp)
        assert result == {"gpu": True, "name": "my_job"}

    def test_base_merges_deeply(self, aj_home):
        write_yaml(aj_home / "base.yaml", {
            "config": {"jobs": [{"name": "default", "sku": "small"}]},
        })
        write_yaml(aj_home / "child.yaml", {
            "base": "base",
            "config": {"jobs": [{"command": ["echo hi"]}]},
        })
        result = read_conf(aj_home / "child.yaml")
        assert result["jobs"] == [{"name": "default", "sku": "small", "command": ["echo hi"]}]


class TestReadConfCircularInheritance:
    def test_self_reference_detected(self, aj_home):
        fp = aj_home / "self.yaml"
        write_yaml(fp, {"base": "self", "config": {"key": "value"}})
        with pytest.raises(ConfigError, match="Circular template inheritance"):
            read_conf(fp)

    def test_mutual_reference_detected(self, aj_home):
        write_yaml(aj_home / "a.yaml", {"base": "b", "config": {"from": "a"}})
        write_yaml(aj_home / "b.yaml", {"base": "a", "config": {"from": "b"}})
        with pytest.raises(ConfigError, match="Circular template inheritance"):
            read_conf(aj_home / "a.yaml")

    def test_three_way_cycle_detected(self, aj_home):
        write_yaml(aj_home / "x.yaml", {"base": "y", "config": {}})
        write_yaml(aj_home / "y.yaml", {"base": "z", "config": {}})
        write_yaml(aj_home / "z.yaml", {"base": "x", "config": {}})
        with pytest.raises(ConfigError, match="Circular template inheritance"):
            read_conf(aj_home / "x.yaml")


class TestReadConfEdgeCases:
    def test_invalid_base_type_raises(self, aj_home):
        fp = aj_home / "bad_base.yaml"
        write_yaml(fp, {"base": 42, "config": {"key": "value"}})
        with pytest.raises(ConfigError, match="must be a string or list"):
            read_conf(fp)

    def test_yaml_only_comments_returns_empty(self, aj_home):
        fp = aj_home / "comments.yaml"
        fp.write_text("# just a comment\n")
        assert read_conf(fp) == {}

    def test_base_not_found_raises(self, aj_home):
        fp = aj_home / "orphan.yaml"
        write_yaml(fp, {"base": "nonexistent", "config": {"key": "value"}})
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            read_conf(fp)
