"""Utilities for string manipulation."""

import re


def remove_accents(old: str) -> str:
    """Remove common accent characters, lower form.

    Args:
        old: Original string to manipulate.
    Returns:
        A string representation from `old` without accents.
    """
    new = old.lower()
    new = re.sub(r"[àáâãäå]", "a", new)
    new = re.sub(r"[èéêë]", "e", new)
    new = re.sub(r"[ìíîï]", "i", new)
    new = re.sub(r"[òóôõö]", "o", new)
    new = re.sub(r"[ùúûü]", "u", new)

    return new
