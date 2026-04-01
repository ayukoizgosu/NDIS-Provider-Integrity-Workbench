from __future__ import annotations

import re
from difflib import SequenceMatcher

COMMON_COMPANY_SUFFIXES = {
    "australia",
    "australian",
    "co",
    "company",
    "corporation",
    "inc",
    "incorporated",
    "limited",
    "ltd",
    "proprietary",
    "pty",
}

CORPORATE_MARKERS = {
    "association",
    "care",
    "community",
    "company",
    "cooperative",
    "disability",
    "foundation",
    "group",
    "health",
    "holdings",
    "inc",
    "incorporated",
    "limited",
    "ltd",
    "organisation",
    "organization",
    "partner",
    "partners",
    "proprietary",
    "provider",
    "pty",
    "service",
    "services",
    "solution",
    "solutions",
    "support",
    "supports",
    "trust",
    "trustee",
}

ALIAS_MARKER_PATTERN = re.compile(
    r"(?i)\b(?:also known as|aka|a\.k\.a\.|alias|trading as|t/as)\b"
)
HONORIFIC_PATTERN = re.compile(r"(?i)^(mr|mrs|ms|miss|dr)\s+")


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def digits_only(value: object) -> str:
    return re.sub(r"\D+", "", normalize_text(value))


def clean_abn(value: object) -> str:
    digits = digits_only(value)
    return digits if len(digits) == 11 else ""


def clean_acn(value: object) -> str:
    digits = digits_only(value)
    return digits if len(digits) == 9 else ""


def _normalize_company_tokens(name: object) -> list[str]:
    text = normalize_text(name).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [token for token in text.split() if token]
    return tokens


def canonical_company_name(name: object) -> str:
    tokens = _normalize_company_tokens(name)
    stripped = [token for token in tokens if token not in COMMON_COMPANY_SUFFIXES]
    if stripped:
        tokens = stripped
    return " ".join(tokens)


def looks_corporate_name(name: object) -> bool:
    tokens = set(_normalize_company_tokens(name))
    return bool(tokens & CORPORATE_MARKERS)


def has_alias_markers(name: object) -> bool:
    text = normalize_text(name)
    return bool(ALIAS_MARKER_PATTERN.search(text))


def strip_honorific(name: object) -> str:
    return HONORIFIC_PATTERN.sub("", normalize_text(name))


def generate_name_variants(name: object) -> list[str]:
    raw = normalize_text(name)
    if not raw:
        return []

    variants: list[str] = []
    canonical = canonical_company_name(raw)
    ascii_variant = re.sub(r"[^A-Za-z0-9 ]+", " ", raw)
    collapsed = re.sub(r"\s+", " ", ascii_variant).strip()

    for candidate in [raw, collapsed, canonical]:
        cleaned = normalize_text(candidate)
        if cleaned and cleaned not in variants:
            variants.append(cleaned)

    if canonical:
        title_variant = canonical.title()
        if title_variant not in variants:
            variants.append(title_variant)

    return variants


def extract_alias_variants(name: object) -> list[str]:
    raw = normalize_text(name)
    if not raw:
        return []

    if not has_alias_markers(raw) and ";" not in raw:
        return []

    working = raw
    working = ALIAS_MARKER_PATTERN.sub(";", working)
    working = re.sub(r"(?i)[()]", ";", working)

    parts: list[str] = []
    for chunk in working.split(";"):
        cleaned_chunk = normalize_text(chunk.strip(" ,.-"))
        if not cleaned_chunk:
            continue

        subparts = [cleaned_chunk]
        if " and " in cleaned_chunk.lower() and not looks_corporate_name(cleaned_chunk):
            subparts = [
                normalize_text(part.strip(" ,.-"))
                for part in re.split(r"(?i)\band\b", cleaned_chunk)
                if normalize_text(part.strip(" ,.-"))
            ]

        for part in subparts:
            normalized_part = strip_honorific(part)
            if normalized_part and normalized_part not in parts:
                parts.append(normalized_part)

    variants: list[str] = []
    for part in parts:
        for candidate in generate_name_variants(part):
            if candidate and candidate not in variants:
                variants.append(candidate)

    return variants


def token_overlap_score(left: object, right: object) -> float:
    left_tokens = set(canonical_company_name(left).split())
    right_tokens = set(canonical_company_name(right).split())
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens & right_tokens
    union = left_tokens | right_tokens
    return len(intersection) / len(union)


def name_similarity_score(left: object, right: object) -> float:
    left_name = canonical_company_name(left)
    right_name = canonical_company_name(right)
    if not left_name or not right_name:
        return 0.0
    sequence_score = SequenceMatcher(None, left_name, right_name).ratio()
    overlap_score = token_overlap_score(left_name, right_name)
    return round((0.7 * sequence_score) + (0.3 * overlap_score), 4)
