"""Secret-oriented text redaction shared by bundled skill scripts."""

from __future__ import annotations

import re
from typing import Any

_URL_QUERY = re.compile(r"(https?://[^?\s]+)\?[^\s]+", re.IGNORECASE)
_URL_USERINFO = re.compile(
    r"(https?://)[^/@\s]+@",
    re.IGNORECASE,
)
_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_AUTHORIZATION = re.compile(
    r"\bAuthorization\s*[:=]\s*[^\r\n]+",
    re.IGNORECASE,
)
_ASSIGNMENT = re.compile(
    r"\b("
    r"[A-Za-z0-9_]*(?:API_KEY|ACCESS_TOKEN|AUTH_TOKEN|TOKEN|PASSWORD|PASSWD|"
    r"SECRET|CLIENTSECRET|CLIENT_SECRET|SAS_TOKEN|SAS|SIG|ACCOUNTKEY|"
    r"ACCOUNT_KEY|SHAREDACCESSKEY|SHARED_ACCESS_KEY|SHAREDACCESSSIGNATURE|"
    r"CONNECTIONSTRING|PRIVATE_KEY|AUTHORIZATION)"
    r"[A-Za-z0-9_]*"
    r")\s*([:=])\s*(\"[^\"]*\"|'[^']*'|[^\s,;]+)",
    re.IGNORECASE,
)
_JSON_SECRET = re.compile(
    r"(?P<key_quote>[\"'])"
    r"(?P<key>[A-Za-z0-9_]*(?:API_KEY|ACCESS_TOKEN|AUTH_TOKEN|TOKEN|PASSWORD|"
    r"PASSWD|SECRET|CLIENTSECRET|CLIENT_SECRET|SAS_TOKEN|SAS|SIG|ACCOUNTKEY|"
    r"ACCOUNT_KEY|SHAREDACCESSKEY|SHARED_ACCESS_KEY|SHAREDACCESSSIGNATURE|"
    r"CONNECTIONSTRING|PRIVATE_KEY|AUTHORIZATION)[A-Za-z0-9_]*)"
    r"(?P=key_quote)\s*:\s*"
    r"(?P<value_quote>[\"'])"
    r"(?P<value>(?:\\.|(?!(?P=value_quote)).)*)"
    r"(?P=value_quote)",
    re.IGNORECASE,
)


_ESCAPED_JSON_SECRET = re.compile(
    r"(?P<key_quote>\\[\"'])"
    r"(?P<key>[A-Za-z0-9_]*(?:API_KEY|ACCESS_TOKEN|AUTH_TOKEN|TOKEN|PASSWORD|"
    r"PASSWD|SECRET|CLIENTSECRET|CLIENT_SECRET|SAS_TOKEN|SAS|SIG|ACCOUNTKEY|"
    r"ACCOUNT_KEY|SHAREDACCESSKEY|SHARED_ACCESS_KEY|SHAREDACCESSSIGNATURE|"
    r"CONNECTIONSTRING|PRIVATE_KEY|AUTHORIZATION)[A-Za-z0-9_]*)"
    r"(?P=key_quote)\s*:\s*"
    r"(?P<value_quote>\\[\"'])"
    r"(?P<value>.*?)"
    r"(?<!\\)(?P=value_quote)",
    re.IGNORECASE,
)
_PEM_BLOCK = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*(?:PRIVATE KEY|CERTIFICATE))-----"
    r".*?"
    r"-----END (?P=label)-----",
    re.DOTALL,
)
_KNOWN_TOKEN = re.compile(
    r"\b(?:"
    r"gh[pousr]_[A-Za-z0-9_]{20,}|"
    r"github_pat_[A-Za-z0-9_]{20,}|"
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
    r")\b"
)


def _replace_escaped_json(match: re.Match[str]) -> str:
    return (
        f"{match.group('key_quote')}{match.group('key')}"
        f"{match.group('key_quote')}: "
        f"{match.group('value_quote')}[REDACTED]"
        f"{match.group('value_quote')}"
    )


def redact_text(value: str) -> str:
    text = _PEM_BLOCK.sub("[REDACTED PEM BLOCK]", str(value))
    text = _URL_USERINFO.sub(r"\1[REDACTED]@", text)
    text = _URL_QUERY.sub(r"\1?[REDACTED]", text)
    text = _BEARER.sub("Bearer [REDACTED]", text)
    text = _AUTHORIZATION.sub("Authorization: [REDACTED]", text)
    text = _ESCAPED_JSON_SECRET.sub(_replace_escaped_json, text)
    text = _JSON_SECRET.sub(
        lambda match: (
            f"{match.group('key_quote')}{match.group('key')}"
            f"{match.group('key_quote')}: "
            f"{match.group('value_quote')}[REDACTED]"
            f"{match.group('value_quote')}"
        ),
        text,
    )
    text = _ASSIGNMENT.sub(r"\1\2[REDACTED]", text)
    return _KNOWN_TOKEN.sub("[REDACTED TOKEN]", text)


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: redact_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def bounded_tail(value: str, lines: int) -> str:
    if lines < 1:
        return ""
    selected = redact_text(str(value)).splitlines()[-lines:]
    return "\n".join(line[:2000] for line in selected)
