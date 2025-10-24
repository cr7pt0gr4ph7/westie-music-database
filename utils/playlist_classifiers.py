"""Utilities for extracting calendar dates and BPM ranges from playlist names."""

from typing import NamedTuple
import polars as pl

from utils.keyword_data import load_keyword_aliases

###################
# Keywords / Tags #
###################


def _create_regex_for_term(term: str) -> str:
    escaped_term = pl.escape_regex(term)
    # Ignore additional whitespaces (e.g. "a b" should also match "a  b")
    escaped_term = escaped_term.replace(' ', ' +')
    return f'\\b{escaped_term}\\b'


def _extract_tags(expr: pl.Expr, tags_to_extract: dict[str, list[str]]) -> pl.Expr:
    all_keywords_alts = '|'.join([_create_regex_for_term(term) for term in tags_to_extract])
    all_keywords_regex = f'(?i)({all_keywords_alts})'

    # Use regexes to extract the keywords, then match the
    # extracted strings against our dictionary to check
    # if the matched keyword should be aliased to something else
    return expr\
        .str.extract_all(all_keywords_regex)\
        .list.eval(pl.element()
                   .str.to_lowercase()
                   .replace_strict(tags_to_extract,
                                   default=pl.lit([], dtype=pl.List(pl.String)),
                                   return_dtype=pl.List(pl.String))
                   .explode())


def extract_tags_from_name(expr: pl.Expr) -> pl.Expr:
    """"Extract a list of tags from the given playlist name."""
    alias_to_tags, negated_alias_to_tags = load_keyword_aliases()

    tags_to_include: pl.Expr = _extract_tags(expr, alias_to_tags)
    tags_to_exclude: pl.Expr = _extract_tags(expr, negated_alias_to_tags)

    return tags_to_include\
        .list.set_difference(tags_to_exclude)\
        .list.unique()\
        .list.sort()


###########################################################
# Patterns for detecting calendar dates in playlist names #
###########################################################


def date_part(name: str, pattern: str):
    # TODO: The query crashes with an OOM when named groups are used...
    # return f'(?<{name}>{pattern})'
    return pattern


yy = date_part('year', r'\d{2}')
yyyy = date_part('year', r'(?:19|20)\d{2}')
mm = date_part('month', r'(?:0[1-9]|1[0-2])')
MMM = date_part('month', r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)')
dd = date_part('day', r'(?:0[1-9]|[12]\d|3[01])')


def date_pattern(name: str, pattern: str):
    return f'\\b{pattern}\\b'


pattern_yyyy_mm_dd = date_pattern('yyyy_mm_dd', f'{yyyy}[-/.]{mm}[-/.]{dd}')
pattern_yyyy_dd_mm = date_pattern('yyyy_dd_mm', f'{yyyy}[-/.]{dd}[-/.]{mm}')
pattern_dd_mm_yyyy = date_pattern('dd_mm_yyyy', f'{dd}[-/.]{mm}[-/.]{yyyy}')
pattern_mm_dd_yyyy = date_pattern('mm_dd_yyyy', f'{mm}[-/.]{dd}[-/.]{yyyy}')

pattern_yy_mm_dd = date_pattern('yy_mm_dd', f'{yy}[-/.]{mm}[-/.]{dd}')
pattern_yy_dd_mm = date_pattern('yy_dd_mm', f'{yy}[-/.]{dd}[-/.]{mm}')
pattern_dd_mm_yy = date_pattern('dd_mm_yy', f'{dd}[-/.]{mm}[-/.]{yy}')
pattern_mm_dd_yy = date_pattern('mm_dd_yy', f'{mm}[-/.]{dd}[-/.]{yy}')

pattern_dd_MMM_yyyy = date_pattern('dd_MMM_yyyy', f'{dd}[-/. ]?{MMM}[-/. ]?{yyyy}')
pattern_MMM_dd_yyyy = date_pattern('MMM_dd_yyyy', f'{MMM}[-/. ]?{dd}[-/. ]?{yyyy}')
pattern_yyyy_MMM_dd = date_pattern('yyyy_MMM_dd', f'{yyyy}[-/. ]?{MMM}[-/. ]?{dd}')
pattern_yyyy_dd_MMM = date_pattern('yyyy_dd_MMM', f'{yyyy}[-/. ]?{dd}[-/. ]?{MMM}')

pattern_dd_MMM_yy = date_pattern('dd_MMM_yy', f'{dd}[-/. ]?{MMM}[-/. ]?{yy}')
pattern_MMM_dd_yy = date_pattern('MMM_dd_yy', f'{MMM}[-/. ]?{dd}[-/. ]?{yy}')
pattern_yy_MMM_dd = date_pattern('yy_MMM_dd', f'{yy}[-/. ]?{MMM}[-/. ]?{dd}')
pattern_yy_dd_MMM = date_pattern('yy_dd_MMM', f'{yy}[-/. ]?{dd}[-/. ]?{MMM}')

pattern_mm_yy = date_pattern('mm_yy', f'{mm}[-/. ]{yy}')
pattern_dd_mm = date_pattern('dd_mm', f'{dd}[-/. ]{mm}')
pattern_yy_mm = date_pattern('yy_mm', f'{yy}[-/. ]{mm}')
pattern_mm_dd = date_pattern('mm_dd', f'{mm}[-/. ]{dd}')

pattern_month_year_or_reversed = date_pattern('month_year_or_reversed', f'(?:{MMM}[a-z]* {yyyy}|{yyyy} {MMM}[a-z]*)')

patterns_date = [
    pattern_yyyy_mm_dd,
    pattern_yyyy_dd_mm,
    pattern_dd_mm_yyyy,
    pattern_mm_dd_yyyy,

    pattern_yy_mm_dd,
    pattern_yy_dd_mm,
    pattern_dd_mm_yy,
    pattern_mm_dd_yy,

    pattern_dd_MMM_yyyy,
    pattern_MMM_dd_yyyy,
    pattern_yyyy_MMM_dd,
    pattern_yyyy_dd_MMM,

    pattern_dd_MMM_yy,
    pattern_yy_MMM_dd,
    # pattern_MMM_dd_yy,  # matches on Jul 2024 as a date :(
    # pattern_yy_dd_MMM,  # matches on 2024 Jul as a date :(

    # pattern_mm_yy,
    # pattern_dd_mm,
    # pattern_yy_mm,
    # pattern_mm_dd,
]


def extract_dates_from_name(playlist_name: pl.Expr, *, sort: bool = False):
    """"Extract a list of calendar dates from the given playlist name."""
    result = pl.concat_list([
        playlist_name.str.extract_all(date_pattern) for date_pattern in patterns_date
    ]).list.unique()

    return result.list.sort() if sort else result


#######################################################
# Patterns for detecting BPM ranges in playlist names #
#######################################################


pattern_bpm_range = r'(\d{2,3})\s*[-–]\s*(\d{2,3})\s*(?:bpm|BPM)?'  # 70 – 79bpm
pattern_bpm_appx = r'[~≈]\s*(\d{2,3})\s*(?:bpm|BPM)?'  # ~100bpm
pattern_bpm_relational = r'[<>]=?\s*(\d{2,3})\s*(?:bpm|BPM)?'  # >120 BPM
pattern_bpm_mention = r'(?:bpm|BPM)[^\d]{0,5}(\d{2,3})'  # bpm 105
pattern_bpm_loose_fallback = r'\b(\d{2,3})\s*(?:bpm|BPM)\b'  # 117 BPM”


def extract_bpm_from_name(playlist_name: pl.Expr):
    """"Extract a list of possible BPM specifications from the given playlist name."""
    raise NotImplementedError()
