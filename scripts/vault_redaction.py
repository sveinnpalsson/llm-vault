#!/usr/bin/env python3
"""Persistent redaction helpers for llm-vault."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from vault_service_defaults import DEFAULT_LOCAL_MODEL_BASE_URL

REDACTION_POLICY_VERSION = "2026-03-22-precision-2"

_EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", flags=re.I)
_PHONE_PATTERN = re.compile(
    r"\b(?:\+?\d{1,3}[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}\b"
)
_URL_PATTERN = re.compile(r"\bhttps?://[^\s)]+", flags=re.I)
_PERSON_NAME_TOKEN = r"[A-Z][a-z]+(?:['’-][A-Z][a-z]+)*"
_LABELED_PERSON_PATTERN = re.compile(
    rf"\b(?i:(?:name|full name|contact|billing contact|employee|employee name|applicant|insured|patient|beneficiary|recipient|attn|attention|owner|signatory))[#:\- \t]+(({_PERSON_NAME_TOKEN})(?:[ \t]+({_PERSON_NAME_TOKEN})){{1,3}})\b"
)
_LABELED_ACCOUNT_PATTERN = re.compile(
    r"\b(?:acct|account|iban|routing|card|ssn)(?:\s+(?:number|no|num))?[:#\s-]*([A-Z0-9][A-Z0-9 -]{2,}\d[A-Z0-9 -]{1,})\b",
    flags=re.I,
)
_LONG_DIGITS_PATTERN = re.compile(r"\b\d{12,19}\b")
_RE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (_EMAIL_PATTERN, "EMAIL"),
    (
        _PHONE_PATTERN,
        "PHONE",
    ),
    (_URL_PATTERN, "URL"),
    (_LONG_DIGITS_PATTERN, "ACCOUNT"),
]

_ALLOWED_MODES = {"regex", "model", "hybrid"}
_KNOWN_KEYS = {"EMAIL", "PHONE", "URL", "ACCOUNT", "PERSON", "ADDRESS", "CUSTOM"}
_GENERIC_LABELS = {
    "",
    ".",
    "..",
    "...",
    "-",
    "--",
    "---",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "unspecified",
    "redacted",
    "placeholder",
    "value",
    "field",
    "form",
    "entry",
    "text",
    "data",
    "name",
    "full name",
    "first name",
    "last name",
    "middle name",
    "surname",
    "given name",
    "company name",
    "account",
    "account number",
    "routing number",
    "card number",
    "phone",
    "phone number",
    "mobile",
    "email",
    "email address",
    "address",
    "street",
    "city",
    "state",
    "zip",
    "zip code",
    "postal code",
    "country",
    "signature",
    "dob",
    "date of birth",
}
_GENERIC_PERSON_TOKENS = {
    "name",
    "first",
    "last",
    "middle",
    "full",
    "surname",
    "given",
    "applicant",
    "insured",
    "beneficiary",
    "patient",
    "signature",
    "person",
    "people",
    "individual",
    "individuals",
}
_ADDRESS_HINTS = {
    "street",
    "st",
    "avenue",
    "ave",
    "road",
    "rd",
    "drive",
    "dr",
    "lane",
    "ln",
    "boulevard",
    "blvd",
    "court",
    "ct",
    "circle",
    "cir",
    "highway",
    "hwy",
    "parkway",
    "pkwy",
    "suite",
    "ste",
    "unit",
    "apt",
    "apartment",
    "box",
    "po",
}
_STATE_OR_REGION_CODES = {
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "dc",
    "de",
    "fl",
    "ga",
    "hi",
    "ia",
    "id",
    "il",
    "in",
    "ks",
    "ky",
    "la",
    "ma",
    "md",
    "me",
    "mi",
    "mn",
    "mo",
    "ms",
    "mt",
    "nc",
    "nd",
    "ne",
    "nh",
    "nj",
    "nm",
    "nv",
    "ny",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "va",
    "vt",
    "wa",
    "wi",
    "wv",
    "wy",
}
DEFAULT_REDACTION_BASE_URL = DEFAULT_LOCAL_MODEL_BASE_URL
DEFAULT_REDACTION_MODEL = "qwen3-14b"
DEFAULT_REDACTION_TIMEOUT_SECONDS = 45
DEFAULT_REDACTION_PROFILE = "standard"
DEFAULT_REDACTION_INSTRUCTION = ""


def _is_local_url(url: str) -> bool:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.startswith("127.")


@dataclass
class RedactionConfig:
    mode: str = "hybrid"
    profile: str = DEFAULT_REDACTION_PROFILE
    instruction: str = DEFAULT_REDACTION_INSTRUCTION
    enabled: bool = True
    base_url: str = DEFAULT_REDACTION_BASE_URL
    model: str = DEFAULT_REDACTION_MODEL
    api_key: str = "local"
    timeout_seconds: int = DEFAULT_REDACTION_TIMEOUT_SECONDS


@dataclass(slots=True)
class RedactionCandidate:
    key_name: str
    value: str
    source: str


@dataclass(slots=True)
class RedactionRunResult:
    chunk_text_redacted: list[str]
    inserted_entries: list[dict[str, str]]
    entries_total: int
    items_redacted: int


@dataclass(slots=True)
class PersistentRedactionMap:
    value_to_placeholder: dict[str, str] = field(default_factory=dict)
    placeholder_to_value: dict[str, str] = field(default_factory=dict)
    placeholder_to_key: dict[str, str] = field(default_factory=dict)
    key_counts: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_rows(cls, rows: list[tuple[str, str, str, str]]):
        obj = cls()
        for key_name, placeholder, value_norm, original_value in rows:
            key = _normalize_key_name(key_name)
            if not is_redaction_value_allowed(key, original_value):
                continue
            obj.value_to_placeholder[str(value_norm)] = str(placeholder)
            obj.placeholder_to_value[str(placeholder)] = str(original_value)
            obj.placeholder_to_key[str(placeholder)] = key
            m = re.search(r"_([A-Z]+)>$", str(placeholder))
            if m:
                idx = _alpha_token_to_int(m.group(1))
                obj.key_counts[key] = max(obj.key_counts.get(key, 0), idx)
        return obj

    def register(self, key_name: str, value: str) -> tuple[str, str, bool]:
        key = _normalize_key_name(key_name)
        if not is_redaction_value_allowed(key, value):
            return "", "", False
        normalized = _normalize_value(key, value)
        if not normalized:
            return "", "", False

        existing = self.value_to_placeholder.get(normalized)
        if existing:
            return existing, normalized, False

        next_count = self.key_counts.get(key, 0) + 1
        self.key_counts[key] = next_count
        placeholder = f"<REDACTED_{key}_{_ordinal_token(next_count)}>"
        self.value_to_placeholder[normalized] = placeholder
        self.placeholder_to_value[placeholder] = value
        self.placeholder_to_key[placeholder] = key
        return placeholder, normalized, True

    def apply(self, text: str) -> str:
        if not text:
            return ""
        out = text
        for placeholder, value in sorted(
            self.placeholder_to_value.items(),
            key=lambda item: len(item[1]),
            reverse=True,
        ):
            exact_pattern = re.compile(re.escape(value), flags=re.I)
            out = exact_pattern.sub(placeholder, out)
            whitespace_pattern = _compile_whitespace_tolerant_pattern(value)
            if whitespace_pattern is not None:
                out = whitespace_pattern.sub(placeholder, out)
            out = _replace_partial_boundary(out, value, placeholder)
        return out

    def unredact(self, text: str) -> str:
        if not text:
            return ""
        out = text
        for placeholder in sorted(self.placeholder_to_value, key=len, reverse=True):
            out = out.replace(placeholder, self.placeholder_to_value[placeholder])
        return out

    def state_signature(self) -> str:
        entries: list[tuple[str, str, str, str]] = []
        for value_norm, placeholder in self.value_to_placeholder.items():
            entries.append(
                (
                    str(self.placeholder_to_key.get(placeholder) or ""),
                    str(placeholder),
                    str(value_norm),
                    str(self.placeholder_to_value.get(placeholder) or ""),
                )
            )
        payload = {
            "entries": sorted(entries),
            "policy_version": REDACTION_POLICY_VERSION,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def render_redacted_text(
    text: str,
    *,
    mode: str,
    table: PersistentRedactionMap,
) -> str:
    selected_mode = (mode or "hybrid").strip().lower()
    if selected_mode not in _ALLOWED_MODES:
        selected_mode = "hybrid"
    out = table.apply(text)
    if selected_mode in {"regex", "hybrid"}:
        out = _regex_final_pass(out)
    return out


def _normalize_key_name(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", (raw or "").strip().upper()).strip("_")
    return cleaned or "CUSTOM"


def _normalize_value(key_name: str, value: str) -> str:
    stripped = (value or "").strip()
    if not stripped:
        return ""
    if key_name in {"PHONE"}:
        digits = re.sub(r"\D", "", stripped)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        return digits
    if key_name in {"URL", "EMAIL"}:
        return stripped.lower().rstrip("/")
    return re.sub(r"\s+", " ", stripped).lower()


def _normalize_candidate_display(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", (value or "").strip())
    cleaned = cleaned.strip("`\"'[]{}()")
    return cleaned.strip()


def _looks_like_generic_label(value: str) -> bool:
    cleaned = _normalize_candidate_display(value).lower().strip(" .,:;")
    if not cleaned:
        return True
    if cleaned in _GENERIC_LABELS:
        return True
    if re.fullmatch(r"[_\-. ]+", cleaned):
        return True
    if cleaned.endswith(":") and cleaned[:-1].strip() in _GENERIC_LABELS:
        return True
    return False


def _candidate_present_in_text(key_name: str, value: str, text: str) -> bool:
    source = str(text or "")
    if not source:
        return False
    display = _normalize_candidate_display(value)
    if not display:
        return False
    if key_name in {"PHONE", "ACCOUNT"}:
        digits = re.sub(r"\D", "", display)
        if digits:
            compact_source = re.sub(r"\D", "", source)
            return digits in compact_source
    if key_name in {"EMAIL", "URL"}:
        return _normalize_value(key_name, display) in _normalize_value(key_name, source)
    return display.lower() in source.lower()


def is_redaction_value_allowed(
    key_name: str,
    value: str,
    *,
    source_text: str | None = None,
) -> bool:
    key = _normalize_key_name(key_name)
    if key not in _KNOWN_KEYS:
        return False

    display = _normalize_candidate_display(value)
    if not display or _looks_like_generic_label(display):
        return False
    if len(display) < 3:
        return False
    if re.fullmatch(r"[Xx*#._-]+", display):
        return False
    if source_text is not None and not _candidate_present_in_text(key, display, source_text):
        return False

    if key == "EMAIL":
        return bool(_EMAIL_PATTERN.fullmatch(display))

    if key == "PHONE":
        digits = _normalize_value(key, display)
        if not digits.isdigit():
            return False
        if len(digits) == 10:
            return digits[0] in "23456789" and digits[3] in "23456789"
        return 11 <= len(digits) <= 15

    if key == "URL":
        return bool(_URL_PATTERN.fullmatch(display))

    if key == "ACCOUNT":
        digits = re.sub(r"\D", "", display)
        compact = re.sub(r"[\s-]+", "", display)
        if len(digits) >= 6:
            return True
        if len(compact) >= 8 and sum(ch.isdigit() for ch in compact) >= 4:
            return True
        return False

    if key == "PERSON":
        if any(ch.isdigit() for ch in display):
            return False
        lowered = display.lower()
        if lowered in _GENERIC_LABELS:
            return False
        if ":" in display or "@" in display or "/" in display:
            return False
        tokens = re.findall(r"[A-Za-z][A-Za-z'’-]*", display)
        if len(tokens) < 2:
            return False
        if any(token.lower() in _GENERIC_PERSON_TOKENS for token in tokens):
            return False
        if sum(len(token) for token in tokens) < 5:
            return False
        return True

    if key == "ADDRESS":
        lowered = display.lower().strip(" .,:;")
        if lowered in _GENERIC_LABELS or lowered in _STATE_OR_REGION_CODES:
            return False
        if len(display) < 8:
            return False
        if re.fullmatch(r"[A-Za-z]{2,3}", display):
            return False
        has_digit = any(ch.isdigit() for ch in display)
        word_tokens = {
            token.lower().strip(".,")
            for token in re.findall(r"[A-Za-z0-9#]+", display)
        }
        if has_digit:
            return True
        if "po" in word_tokens and "box" in word_tokens:
            return True
        if word_tokens & _ADDRESS_HINTS:
            return True
        return False

    if key == "CUSTOM":
        return False

    return False


def _ordinal_token(n: int) -> str:
    chars: list[str] = []
    x = n
    while x > 0:
        x -= 1
        chars.append(chr(ord("A") + (x % 26)))
        x //= 26
    return "".join(reversed(chars))


def _alpha_token_to_int(token: str) -> int:
    out = 0
    for char in token:
        if not ("A" <= char <= "Z"):
            return 0
        out = (out * 26) + (ord(char) - ord("A") + 1)
    return out


def _replace_partial_boundary(text: str, value: str, placeholder: str) -> str:
    if not text or not value:
        return text

    target = value.lower()
    best = min(4, len(target) - 1) if len(target) > 1 else 0
    if best <= 0:
        return text

    lowered = text.lower()
    for k in range(len(target) - 1, best - 1, -1):
        prefix = target[:k]
        if lowered.endswith(prefix):
            text = text[: len(text) - k] + placeholder
            lowered = text.lower()
            break

    for k in range(len(target) - 1, best - 1, -1):
        suffix = target[len(target) - k :]
        if lowered.startswith(suffix):
            text = placeholder + text[k:]
            break
    return text


def _compile_whitespace_tolerant_pattern(value: str) -> re.Pattern[str] | None:
    normalized = _normalize_candidate_display(value)
    if not normalized or len(normalized.split()) < 2:
        return None
    if not re.search(r"\s", value):
        return None

    parts = [part for part in normalized.split(" ") if part]
    if len(parts) < 2:
        return None

    body = r"\s+".join(re.escape(part) for part in parts)
    prefix = r"(?<!\w)" if re.match(r"^\w", parts[0]) else ""
    suffix = r"(?!\w)" if re.search(r"\w$", parts[-1]) else ""
    return re.compile(f"{prefix}{body}{suffix}", flags=re.I)


def _regex_detect_candidates(text: str) -> list[RedactionCandidate]:
    out: list[RedactionCandidate] = []
    occupied_spans: list[tuple[int, int]] = []

    def _overlaps(span: tuple[int, int]) -> bool:
        start, end = span
        for seen_start, seen_end in occupied_spans:
            if start < seen_end and end > seen_start:
                return True
        return False

    def _normalize_labeled_account_value(value: str) -> str:
        tokens = [token for token in re.split(r"\s+", (value or "").strip()) if token]
        kept: list[str] = []
        for token in tokens:
            if any(ch.isdigit() for ch in token):
                kept.append(token)
                continue
            break
        return " ".join(kept).strip()

    for match in _LABELED_PERSON_PATTERN.finditer(text or ""):
        value = (match.group(1) or "").strip()
        span = match.span(1)
        if value:
            out.append(
                RedactionCandidate(
                    key_name="PERSON",
                    value=value,
                    source="regex",
                )
            )
            occupied_spans.append(span)

    for match in _LABELED_ACCOUNT_PATTERN.finditer(text or ""):
        value = _normalize_labeled_account_value(match.group(1) or "")
        span = match.span(1)
        if value:
            out.append(
                RedactionCandidate(
                    key_name="ACCOUNT",
                    value=value,
                    source="regex",
                )
            )
            occupied_spans.append(span)

    for pattern, key_name in _RE_PATTERNS:
        for match in pattern.finditer(text or ""):
            span = match.span(0)
            if _overlaps(span):
                continue
            value = match.group(0).strip()
            if value and is_redaction_value_allowed(key_name, value, source_text=text):
                out.append(
                    RedactionCandidate(
                        key_name=key_name,
                        value=value,
                        source="regex",
                    )
                )
    return out


def _regex_final_pass(text: str) -> str:
    out = text or ""
    source_text = out
    for pattern, key_name in _RE_PATTERNS:
        out = pattern.sub(
            lambda match: (
                f"<REDACTED_{key_name}>"
                if is_redaction_value_allowed(key_name, match.group(0).strip(), source_text=source_text)
                else match.group(0)
            ),
            out,
        )
    return out


def _strip_code_fences(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)
    return cleaned.strip()


def _extract_first_json(text: str) -> dict[str, Any] | None:
    in_string = False
    escaped = False
    depth = 0
    start: int | None = None
    for idx, char in enumerate(text or ""):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if char == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                candidate = text[start : idx + 1]
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    start = None
                    continue
                if isinstance(parsed, dict):
                    return parsed
                start = None
    return None


def _model_detect_candidates(
    text: str,
    *,
    cfg: RedactionConfig,
    source: str,
) -> list[RedactionCandidate]:
    if not cfg.enabled or not _is_local_url(cfg.base_url):
        return []

    payload = {
        "model": cfg.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Identify PII in text and return JSON only. "
                    "Schema: {\"redactions\":[{\"key_name\":\"EMAIL|PHONE|URL|ACCOUNT|PERSON|ADDRESS|CUSTOM\","
                    "\"values\":[\"...\"]}]}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Profile: {cfg.profile}\n"
                    f"Instruction: {cfg.instruction or 'standard privacy redaction'}\n"
                    f"Text:\n{text}"
                ),
            },
        ],
        "temperature": 0.0,
        "max_tokens": 600,
        "response_format": {"type": "json_object"},
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if cfg.api_key:
        headers["Authorization"] = f"Bearer {cfg.api_key}"
    req = urllib.request.Request(
        f"{cfg.base_url.rstrip('/')}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(1, int(cfg.timeout_seconds))) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            payload.pop("response_format", None)
            req2 = urllib.request.Request(
                f"{cfg.base_url.rstrip('/')}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(req2, timeout=max(1, int(cfg.timeout_seconds))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        else:
            return []
    except Exception:
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    choices = parsed.get("choices") if isinstance(parsed, dict) else None
    if not isinstance(choices, list) or not choices:
        return []
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first, dict) else {}
    content = ""
    if isinstance(message, dict):
        val = message.get("content")
        if isinstance(val, str):
            content = val
        elif isinstance(val, list):
            out_parts: list[str] = []
            for item in val:
                if isinstance(item, str):
                    out_parts.append(item)
                elif isinstance(item, dict):
                    txt = item.get("text")
                    if isinstance(txt, str):
                        out_parts.append(txt)
            content = "".join(out_parts)
    parsed_json = _extract_first_json(_strip_code_fences(content))
    if not isinstance(parsed_json, dict):
        return []
    redactions = parsed_json.get("redactions")
    if not isinstance(redactions, list):
        return []

    out: list[RedactionCandidate] = []
    for item in redactions:
        if not isinstance(item, dict):
            continue
        key_name = str(item.get("key_name") or "CUSTOM").strip() or "CUSTOM"
        values = item.get("values")
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue
        for val in values:
            sval = str(val or "").strip()
            if sval and is_redaction_value_allowed(key_name, sval, source_text=text):
                out.append(
                    RedactionCandidate(
                        key_name=key_name,
                        value=sval,
                        source=source,
                    )
                )
    return out


def redact_chunks_with_persistent_map(
    chunks: list[str],
    *,
    mode: str,
    table: PersistentRedactionMap,
    cfg: RedactionConfig | None = None,
) -> RedactionRunResult:
    selected_mode = (mode or "hybrid").strip().lower()
    if selected_mode not in _ALLOWED_MODES:
        selected_mode = "hybrid"

    inserted_entries: list[dict[str, str]] = []
    redacted_items = 0

    def _register(candidates: list[RedactionCandidate]):
        for cand in candidates:
            placeholder, value_norm, is_new = table.register(cand.key_name, cand.value)
            if placeholder and is_new:
                inserted_entries.append(
                    {
                        "key_name": _normalize_key_name(cand.key_name),
                        "placeholder": placeholder,
                        "value_norm": value_norm,
                        "original_value": cand.value,
                        "source_mode": cand.source,
                    }
                )

    for text in chunks:
        _register(_regex_detect_candidates(text))

    if selected_mode in {"model", "hybrid"} and cfg is not None and cfg.enabled:
        for text in chunks:
            _register(
                _model_detect_candidates(
                    text,
                    cfg=cfg,
                    source="llm_chunk",
                )
            )

    redacted_chunks: list[str] = []
    for text in chunks:
        out = render_redacted_text(text, mode=selected_mode, table=table)
        if out != text:
            redacted_items += 1
        redacted_chunks.append(out)

    return RedactionRunResult(
        chunk_text_redacted=redacted_chunks,
        inserted_entries=inserted_entries,
        entries_total=len(table.value_to_placeholder),
        items_redacted=redacted_items,
    )
