"""Utility functions for dealing with strings."""
from typing import Final

import regex

LOWERCASE_REGEX: Final = regex.compile(r"(?i)\b(and|or|and/or|of|but|[0-9]+s)\b")


def title_case(input: str) -> str:
    """Custom version of `str.title()` that does not capitalize fill words and other things that ought to be lowercase."""
    return LOWERCASE_REGEX.subf(lambda match: match.group(0).lower(), input.title())
