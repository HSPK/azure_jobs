import pytest

from azure_jobs.core.conf import merge_confs


class TestMergeConfsNoneHandling:
    def test_all_none(self):
        assert merge_confs(None, None) is None

    def test_single_none(self):
        assert merge_confs(None) is None

    def test_none_filtered_out_dict(self):
        assert merge_confs(None, {"a": 1}) == {"a": 1}

    def test_none_filtered_out_preserves_order(self):
        assert merge_confs({"a": 1}, None, {"a": 2}) == {"a": 2}

    def test_empty_args(self):
        assert merge_confs() is None


class TestMergeConfsDicts:
    def test_single_dict(self):
        assert merge_confs({"a": 1}) == {"a": 1}

    def test_disjoint_keys(self):
        result = merge_confs({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_overlapping_keys_last_wins(self):
        result = merge_confs({"a": 1}, {"a": 2})
        assert result == {"a": 2}

    def test_nested_dicts_merge_recursively(self):
        result = merge_confs({"x": {"a": 1}}, {"x": {"b": 2}})
        assert result == {"x": {"a": 1, "b": 2}}

    def test_deeply_nested(self):
        d1 = {"a": {"b": {"c": 1, "d": 2}}}
        d2 = {"a": {"b": {"c": 3, "e": 4}}}
        result = merge_confs(d1, d2)
        assert result == {"a": {"b": {"c": 3, "d": 2, "e": 4}}}

    def test_three_dicts(self):
        result = merge_confs({"a": 1}, {"b": 2}, {"a": 3, "c": 4})
        assert result == {"a": 3, "b": 2, "c": 4}

    def test_empty_dict(self):
        assert merge_confs({}, {"a": 1}) == {"a": 1}

    def test_both_empty_dicts(self):
        assert merge_confs({}, {}) == {}


class TestMergeConfsLists:
    def test_scalar_lists_concatenate(self):
        result = merge_confs([1, 2], [3, 4])
        assert result == [1, 2, 3, 4]

    def test_string_lists_concatenate(self):
        result = merge_confs(["a", "b"], ["c"])
        assert result == ["a", "b", "c"]

    def test_lists_with_dicts_merge_by_index(self):
        result = merge_confs([{"a": 1}], [{"b": 2}])
        assert result == [{"a": 1, "b": 2}]

    def test_lists_with_dicts_different_lengths(self):
        result = merge_confs([{"a": 1}, {"b": 2}], [{"c": 3}])
        assert result == [{"a": 1, "c": 3}, {"b": 2}]

    def test_empty_lists(self):
        assert merge_confs([], []) == []


class TestMergeConfsScalars:
    def test_last_value_wins(self):
        assert merge_confs(1, 2) == 2
        assert merge_confs("a", "b") == "b"

    def test_three_scalars(self):
        assert merge_confs(1, 2, 3) == 3

    def test_mixed_types_last_wins(self):
        assert merge_confs({"a": 1}, "override") == "override"

    def test_deep_copy_on_scalar(self):
        original = [1, 2, 3]
        result = merge_confs(original)
        result.append(4)
        assert original == [1, 2, 3]


class TestMergeConfsDeepCopy:
    def test_dict_values_are_independent(self):
        d1 = {"a": {"nested": [1, 2]}}
        result = merge_confs(d1)
        result["a"]["nested"].append(3)
        assert d1["a"]["nested"] == [1, 2]

    def test_list_values_are_independent(self):
        l1 = [1, 2]
        l2 = [3, 4]
        result = merge_confs(l1, l2)
        result.append(5)
        assert l1 == [1, 2]
        assert l2 == [3, 4]
