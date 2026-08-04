"""Unit tests for utils/naming.py."""

from __future__ import annotations

import pytest

from azure_jobs.shared.utils.naming import sanitize_dns1035


class TestSanitizeDns1035:
    def test_replaces_underscores_and_dots(self):
        # The case that triggered Volcano svc-plugin to reject the Service.
        assert (
            sanitize_dns1035("ml_group_v2.0-t2-vq-dyn-brss-rds-b200")
            == "ml-group-v2-0-t2-vq-dyn-brss-rds-b200"
        )

    def test_lowercases_uppercase(self):
        assert sanitize_dns1035("MyJob") == "myjob"

    def test_replaces_arbitrary_non_alnum(self):
        assert sanitize_dns1035("a/b c@d") == "a-b-c-d"

    def test_collapses_consecutive_dashes(self):
        assert sanitize_dns1035("a..b__c") == "a-b-c"

    def test_strips_leading_trailing_dashes(self):
        assert sanitize_dns1035("--foo--") == "foo"
        assert sanitize_dns1035("__bar__") == "bar"

    def test_prefixes_when_starting_with_digit(self):
        # DNS-1035 requires the first character to be a letter.
        assert sanitize_dns1035("123abc").startswith("j-")
        assert sanitize_dns1035("9-something").startswith("j-")

    def test_empty_input_falls_back(self):
        assert sanitize_dns1035("") == "job"
        assert sanitize_dns1035("___") == "job"
        assert sanitize_dns1035("...") == "job"

    def test_truncation_with_trailing_dash_stripped(self):
        # max_length 10, but a trailing '-' after truncation should be stripped
        # so the name still ends with an alphanumeric.
        out = sanitize_dns1035("aaaaaaaaa-bbbbb", max_length=10)
        assert out == "aaaaaaaaa"
        assert not out.endswith("-")

    def test_default_max_length_is_63(self):
        long_name = "a" * 100
        assert len(sanitize_dns1035(long_name)) == 63

    def test_starts_with_letter_after_sanitization(self):
        # ``.123`` strips to ``123`` then gets ``j-`` prefix.
        assert sanitize_dns1035(".123") == "j-123"

    def test_idempotent_on_already_valid_names(self):
        for s in ("my-job", "abc123", "x", "abc-123-def"):
            assert sanitize_dns1035(s) == s

    @pytest.mark.parametrize(
        "raw",
        [
            "ml_group_v2.0-t2-vq-dyn-brss-rds-b200_d173cf2a",
            "Job.With.Dots",
            "name with spaces",
            "weird/chars\\here",
            "9starts-with-digit",
            "",
        ],
    )
    def test_output_always_dns1035_compliant(self, raw):
        """The sanitised output must satisfy the regex k8s actually uses."""
        import re

        out = sanitize_dns1035(raw)
        assert re.fullmatch(r"[a-z]([-a-z0-9]*[a-z0-9])?", out) is not None, out
        assert len(out) <= 63
