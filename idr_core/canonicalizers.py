"""
Domain-aware PII canonicalization registry and SQL expression builder for IDR.

Supports:
1. Built-in presets:
   - `NONE` / `EXACT`: Raw trimmed string
   - `LOWERCASE`: Lowercased trimmed string
   - `UPPERCASE`: Uppercased trimmed string
   - `EMAIL_STANDARD`: Trims whitespace and lowercases (preserves +tag sub-addressing)
   - `EMAIL_STRIP_PLUS`: Trims whitespace, lowercases, and strips `+tag` before `@`
   - `PHONE_DIGITS`: Strips all non-numeric characters (`[^0-9]`)
   - `PHONE_US_10`: Strips non-digits and normalizes 11-digit leading-1 North American numbers to 10 digits
   - `ALPHANUMERIC_UPPER`: Strips non-alphanumeric characters (`[^a-zA-Z0-9]`) and uppercases
2. Custom Python Registry:
   - `register_canonicalizer(name, fn)` where `fn(raw_expr: str, dialect: str) -> str`
3. Inline Warehouse SQL Expressions:
   - Any safe SQL expression containing `{val}` or `{col}` (e.g., `REGEXP_REPLACE(LOWER({val}), '^0+', '')`)
"""

from typing import Callable, Dict, List

from .config import validate_sql_expr

CanonicalizerFn = Callable[[str, str], str]


def _regexp_replace_all(expr: str, pattern: str, replacement: str, dialect: str) -> str:
    """Generate dialect-safe global REGEXP_REPLACE SQL expression."""
    if dialect in ("duckdb", "postgres"):
        return f"REGEXP_REPLACE({expr}, '{pattern}', '{replacement}', 'g')"
    return f"REGEXP_REPLACE({expr}, '{pattern}', '{replacement}')"


def _canon_none(raw_expr: str, dialect: str) -> str:
    return raw_expr


def _canon_lowercase(raw_expr: str, dialect: str) -> str:
    return f"LOWER({raw_expr})"


def _canon_uppercase(raw_expr: str, dialect: str) -> str:
    return f"UPPER({raw_expr})"


def _canon_email_standard(raw_expr: str, dialect: str) -> str:
    """Lowercase and trim whitespace (leaves +tag intact)."""
    return f"LOWER(TRIM({raw_expr}))"


def _canon_email_strip_plus(raw_expr: str, dialect: str) -> str:
    """
    Lowercase, trim whitespace, and strip +tag sub-addressing before '@'.
    Uses character class `[+]` instead of backslash escaping for universal dialect compatibility.
    """
    lowered = f"LOWER(TRIM({raw_expr}))"
    return _regexp_replace_all(lowered, "[+][^@]*@", "@", dialect)


def _canon_phone_digits(raw_expr: str, dialect: str) -> str:
    """Strip all non-numeric characters."""
    return _regexp_replace_all(raw_expr, "[^0-9]", "", dialect)


def _canon_phone_us_10(raw_expr: str, dialect: str) -> str:
    """
    Strip non-digits and normalize 11-digit numbers starting with '1' to 10 digits.
    Matches `+1 (555) 123-4567`, `1-555-123-4567`, and `5551234567` to `5551234567`.
    """
    digits = _canon_phone_digits(raw_expr, dialect)
    return (
        f"CASE WHEN LENGTH({digits}) = 11 AND SUBSTR({digits}, 1, 1) = '1' "
        f"THEN SUBSTR({digits}, 2, 10) ELSE {digits} END"
    )


def _canon_alphanumeric_upper(raw_expr: str, dialect: str) -> str:
    """Strip spaces, hyphens, and punctuation, then uppercase (ideal for loyalty/account IDs)."""
    cleaned = _regexp_replace_all(raw_expr, "[^a-zA-Z0-9]", "", dialect)
    return f"UPPER({cleaned})"


_CANONICALIZER_REGISTRY: Dict[str, CanonicalizerFn] = {
    "": _canon_none,
    "NONE": _canon_none,
    "EXACT": _canon_none,
    "LOWERCASE": _canon_lowercase,
    "UPPERCASE": _canon_uppercase,
    "EMAIL_STANDARD": _canon_email_standard,
    "EMAIL_STRIP_PLUS": _canon_email_strip_plus,
    "PHONE_DIGITS": _canon_phone_digits,
    "PHONE_US_10": _canon_phone_us_10,
    "ALPHANUMERIC_UPPER": _canon_alphanumeric_upper,
}


def register_canonicalizer(name: str, fn: CanonicalizerFn) -> None:
    """
    Register a custom canonicalization function.

    Args:
        name: Uppercase identifier name (e.g., 'CUSTOM_TAX_ID')
        fn: Callable `(raw_sql_expr: str, dialect: str) -> str` returning a SQL expression
    """
    if not name or not isinstance(name, str):
        raise ValueError("Canonicalizer name must be a non-empty string")
    if not callable(fn):
        raise ValueError("Canonicalizer fn must be callable(raw_expr, dialect) -> str")
    _CANONICALIZER_REGISTRY[name.strip().upper()] = fn


def list_canonicalizers() -> List[str]:
    """Return sorted list of registered canonicalizer preset names."""
    return sorted(k for k in _CANONICALIZER_REGISTRY.keys() if k)


def is_registered_canonicalizer(name: str) -> bool:
    """Check whether `name` is a registered preset canonicalizer."""
    return str(name or "").strip().upper() in _CANONICALIZER_REGISTRY


def validate_canonicalize_spec(spec: str, field_name: str = "canonicalize") -> str:
    """
    Validate a canonicalize specification (either a registered preset name
    or a custom SQL template containing `{val}` or `{col}`).
    """
    if spec is None:
        return "NONE"
    raw = str(spec).strip()
    upper = raw.upper()
    if upper in _CANONICALIZER_REGISTRY:
        return upper
    if "{val}" in raw or "{col}" in raw:
        validate_sql_expr(raw.replace("{val}", "col").replace("{col}", "col"), field_name)
        return raw
    raise ValueError(
        f"Invalid {field_name}: '{spec}'. Must be a registered canonicalizer "
        f"({', '.join(list_canonicalizers())}) or a custom SQL expression containing '{{val}}'."
    )


def build_canonicalize_expr(raw_expr: str, canonicalize_spec: str, dialect: str = "duckdb") -> str:
    """
    Build the dialect-specific SQL expression for canonicalizing `raw_expr`.

    Args:
        raw_expr: SQL expression producing a trimmed string (e.g., `TRIM(CAST((email) AS VARCHAR))`)
        canonicalize_spec: Preset name (e.g., `'EMAIL_STANDARD'`, `'PHONE_US_10'`) or SQL template with `{val}`
        dialect: Warehouse dialect (`'duckdb'`, `'snowflake'`, `'bigquery'`, `'databricks'`)
    """
    spec = validate_canonicalize_spec(canonicalize_spec)
    upper = spec.upper()
    if upper in _CANONICALIZER_REGISTRY:
        return _CANONICALIZER_REGISTRY[upper](raw_expr, dialect)
    # Custom inline SQL template with {val} or {col}
    return spec.replace("{val}", f"({raw_expr})").replace("{col}", f"({raw_expr})")

