"""Utilities for extracting calendar dates and BPM ranges from playlist names."""

from typing import Final, NamedTuple

import polars as pl
import regex

from utils.keyword_data import load_keyword_data

###################
# Keywords / Tags #
###################


treat_like_whitespace: Final = r'[ \-_.]+'


def _create_regex_for_term(term: str) -> str:
    escaped_term = pl.escape_regex(term)

    # Apply normalization to keywords so the dictionary lookup works afterwards
    escaped_term = regex.sub(treat_like_whitespace, ' ', escaped_term)

    # Ignore additional whitespaces (e.g. "a b" should also match "a  b")
    # Not necessary, as we normalize the input string before matching.
    # escaped_term = escaped_term.replace(' ', r'[ \-_.]+')

    return escaped_term


def _extract_tags(expr: pl.Expr, tags_to_extract: dict[str, list[str]]) -> pl.Expr:
    all_keywords_alts = '|'.join([_create_regex_for_term(term) for term in tags_to_extract])
    optional_year_suffix = r'(?:[0-9]{4})?'
    typographic_dash = '[\u2012\u2013\u2014\u2e3a]'  # used by spotify in auto-generated playlist names
    all_keywords_regex = f'(?i)(?:{typographic_dash}|\\b(?:{all_keywords_alts}){optional_year_suffix}\\b)'

    # Use regexes to extract the keywords, then match the
    # extracted strings against our dictionary to check
    # if the matched keyword should be aliased to something else
    return expr\
        .str.replace_all(treat_like_whitespace, ' ')\
        .str.extract_all(all_keywords_regex)\
        .list.eval(pl.element()
                   .str.to_lowercase()
                   .pipe(lambda expr:
                         pl.concat_list(
                             expr
                             .replace_strict(tags_to_extract,
                                             default=pl.lit([], dtype=pl.List(pl.String)),
                                             return_dtype=pl.List(pl.String)),
                             expr
                             .str.extract_groups(r'^(.+?)(?:20[0-2][0-9])?$')
                             .struct["1"]
                             .replace_strict(tags_to_extract,
                                             default=pl.lit([], dtype=pl.List(pl.String)),
                                             return_dtype=pl.List(pl.String))
                         ))
                   .explode())


def extract_tags_from_name(expr: pl.Expr) -> pl.Expr:
    """"Extract a list of tags from the given playlist name."""
    keywords = load_keyword_data()

    tags_to_include: pl.Expr = _extract_tags(expr, keywords.keywords_to_tags)
    tags_to_exclude: pl.Expr = _extract_tags(expr, keywords.keywords_to_excluded_tags)

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
m = date_part('month', r'(?:[1-9]|1[0-2])')
mm = date_part('month', r'(?:0[1-9]|1[0-2])')
MMM = date_part('month', r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)')
d = date_part('day', r'(?:[1-9]|[12]\d|3[01])')
dd = date_part('day', r'(?:0[1-9]|[12]\d|3[01])')
DDD = date_part('weekday', r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)')
DDDD = date_part('weekday', r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)')


def date_pattern(name: str, pattern: str):
    return (name, f'(?i)\\b{pattern}\\b')


pattern_yyyy_mm_dd = date_pattern('yyyy_mm_dd', f'{yyyy}[-/.]{mm}[-/.]{dd}')
pattern_yyyy_dd_mm = date_pattern('yyyy_dd_mm', f'{yyyy}[-/.]{dd}[-/.]{mm}')
pattern_dd_mm_yyyy = date_pattern('dd_mm_yyyy', f'{dd}[-/.]{mm}[-/.]{yyyy}')
pattern_mm_dd_yyyy = date_pattern('mm_dd_yyyy', f'{mm}[-/.]{dd}[-/.]{yyyy}')

pattern_yyyymmdd = date_pattern('yyyymmdd', f'{yyyy}{mm}{dd}')
pattern_yyyyddmm = date_pattern('yyyyddmm', f'{yyyy}{dd}{mm}')
pattern_ddmmyyyy = date_pattern('ddmmyyyy', f'{dd}{mm}{yyyy}')
pattern_mmddyyyy = date_pattern('mmddyyyy', f'{mm}{dd}{yyyy}')

pattern_yymmdd = date_pattern('yymmdd', f'{yy}{mm}{dd}')
pattern_yyddmm = date_pattern('yyddmm', f'{yy}{dd}{mm}')
pattern_ddmmyy = date_pattern('ddmmyy', f'{dd}{mm}{yy}')
pattern_mmddyy = date_pattern('mmddyy', f'{mm}{dd}{yy}')

pattern_yy_mm_dd = date_pattern('yy_mm_dd', f'{yy}[-/.]{mm}[-/.]{dd}')
pattern_yy_dd_mm = date_pattern('yy_dd_mm', f'{yy}[-/.]{dd}[-/.]{mm}')
pattern_dd_mm_yy = date_pattern('dd_mm_yy', f'{dd}[-/.]{mm}[-/.]{yy}')
pattern_mm_dd_yy = date_pattern('mm_dd_yy', f'{mm}[-/.]{dd}[-/.]{yy}')

pattern_dd_MMM_yyyy = date_pattern('dd_MMM_yyyy', f'{dd}[-/. ]?{MMM}[-/. ]?{yyyy}')
pattern_MMM_dd_yyyy = date_pattern('MMM_dd_yyyy', f'{MMM}[-/. ]?{dd}[-/. ]?{yyyy}')
pattern_yyyy_MMM_dd = date_pattern('yyyy_MMM_dd', f'{yyyy}[-/. ]?{MMM}[-/. ]?{dd}')
pattern_yyyy_dd_MMM = date_pattern('yyyy_dd_MMM', f'{yyyy}[-/. ]?{dd}[-/. ]?{MMM}')

pattern_dd_MMM_yy = date_pattern('dd_MMM_yy', f'{dd}(?:st|nd|rd|th)?[-/. ]?{MMM}[-/. ]?{yy}')
pattern_MMM_dd_yy = date_pattern('MMM_dd_yy', f'{MMM}[-/. ]{"{0,2}"}{dd}(?:st|nd|rd|th)[-/. ]?{yy}')
pattern_yy_MMM_dd = date_pattern('yy_MMM_dd', f'{yy}[-/. ]?{MMM}[-/. ]?{dd}')
pattern_yy_dd_MMM = date_pattern('yy_dd_MMM', f'{yy}[-/. ]?{dd}[-/. ]?{MMM}')

pattern_mm_yy = date_pattern('mm_yy', f'{mm}[-/. ]{yy}')
pattern_dd_mm = date_pattern('dd_mm', f'{dd}[-/. ]{mm}')
pattern_yy_mm = date_pattern('yy_mm', f'{yy}[-/. ]{mm}')
pattern_mm_dd = date_pattern('mm_dd', f'{mm}[-/. ]{dd}')

pattern_d_m_yy = date_pattern('d_m_yy', f'{d}[-/.]{m}[-/.]{yy}')
pattern_m_d_yy = date_pattern('m_d_yy', f'{m}[-/.]{d}[-/.]{yy}')
pattern_d_m_yyyy = date_pattern('d_m_yyyy', f'{d}[-/.]{m}[-/.]{yyyy}')
pattern_m_d_yyyy = date_pattern('m_d_yyyy', f'{m}[-/.]{d}[-/.]{yyyy}')

pattern_DDD_dd_MMM = date_pattern('DDD_dd_MMM', f'{DDD}[., ]+{dd}(?:\\.|st|nd|rd|th)?( of |[-, ]){MMM}')
pattern_DDDD_dd_MMM = date_pattern('DDDD_dd_MMM', f'{DDDD}[., ]+{dd}(?:\\.|st|nd|rd|th)?( of |[-, ]){MMM}')

pattern_month_year_or_reversed = date_pattern('month_year_or_reversed', f'(?:{MMM}[a-z]* {yyyy}|{yyyy} {MMM}[a-z]*)')

patterns_date = [
    pattern_yyyy_mm_dd,
    pattern_yyyy_dd_mm,
    pattern_dd_mm_yyyy,
    pattern_mm_dd_yyyy,

    pattern_yyyymmdd,
    pattern_yyyyddmm,
    pattern_ddmmyyyy,
    pattern_mmddyyyy,

    pattern_yy_mm_dd,
    pattern_yy_dd_mm,
    pattern_dd_mm_yy,
    pattern_mm_dd_yy,

    pattern_yymmdd,
    pattern_yyddmm,
    pattern_ddmmyy,
    pattern_mmddyy,

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

    pattern_d_m_yy,
    pattern_m_d_yy,
    pattern_d_m_yyyy,
    pattern_m_d_yyyy,

    pattern_DDD_dd_MMM,
    pattern_DDDD_dd_MMM,
]


def extract_date_strings_from_name(playlist_name: pl.Expr, *, sort: bool = False):
    """"Extract a list of calendar dates from the given playlist name."""
    result = pl.concat_list([
        playlist_name.str.extract_all(pattern)
        for (name, pattern) in patterns_date
    ]).list.drop_nulls().list.unique()

    return result.list.sort() if sort else result


def extract_date_types_from_name(playlist_name: pl.Expr, *, sort: bool = False):
    """"Extract a list of calendar date typess from the given playlist name."""
    result = pl.concat_list([
        pl.when(playlist_name.str.find(pattern).is_not_null())
          .then(pl.lit(name))
        for (name, pattern) in patterns_date
    ]).list.drop_nulls().list.unique()

    return result.list.sort() if sort else result


def contains_date_in_name(playlist_name: pl.Expr):
    """Returns whether `playlist_name` likely contains a date."""
    return extract_date_strings_from_name(playlist_name).list.len().gt(0)


pattern_MMM_yy = date_pattern('MMM_yy', f'{MMM} {yy}')
pattern_MMM_yyyy = date_pattern('MMM_yyyy', f'{MMM} {yyyy}')

pattern_mm_yy = date_pattern('mm_yy', f'{mm}[/]{yy}')
pattern_mm_yyyy = date_pattern('mm_yyyy', f'{mm}[/]{yyyy}')

pattern_m_yy = date_pattern('mm_yy', f'{m}[/]{yy}')
pattern_m_yyyy = date_pattern('mm_yyyy', f'{m}[/]{yyyy}')

pattern_yyyy_m = date_pattern('mm_yyyy', f'{yyyy}[ ]{m}')
pattern_yyyy_mm = date_pattern('mm_yyyy', f'{yyyy}[ ]{mm}')

patterns_month_year = [
    pattern_MMM_yy,
    pattern_MMM_yyyy,
    pattern_mm_yy,
    pattern_mm_yyyy,
    pattern_m_yy,
    pattern_m_yyyy,
]


def extract_month_year_strings_from_name(playlist_name: pl.Expr, *, sort: bool = False):
    """"Extract a list of calendar dates from the given playlist name."""
    result = pl.concat_list([
        playlist_name.str.extract_all(pattern)
        for (name, pattern) in patterns_month_year
    ]).list.drop_nulls().list.unique()

    return result.list.sort() if sort else result


def contains_month_year_in_name(playlist_name: pl.Expr):
    """Returns whether `playlist_name` likely contains month + year specification."""
    return extract_month_year_strings_from_name(playlist_name).list.len().gt(0)


#######################################################
# Patterns for detecting BPM ranges in playlist names #
#######################################################


class BpmPattern(NamedTuple):
    name: str
    example: str
    pattern: str


pattern_bpm_range = BpmPattern(
    'range', '70 – 79bpm', r'(\d{2,3})\s*([-–]|[aá]|bis|to)\s*(\d{2,3})\s*(?-i:bpms?|beats per minute)?')
pattern_bpm_appx_1 = BpmPattern('approximate', '~100bpm', r'[~≈]\s*(\d{2,3})\s*(?-i:bpms?|beats per minute)?')
pattern_bpm_appx_2 = BpmPattern('approximate', '~100bpm', r'(\d{2,3})-?ish\s*(?-i:bpms?|beats per minute)?')
pattern_bpm_relational = BpmPattern('relational', '>120 BPM', r'[<>]=?\s*(\d{2,3})\s*(?-i:bpms?|beats per minute)?')
pattern_bpm_plus = BpmPattern('plus', '120+ BPM', r'(\d{2,3})(\+|\s+or\s+more\s)\s*(?-i:bpms?|beats per minute)?')
pattern_bpm_mention = BpmPattern('mention', 'bpm 105', r'(?-i:bpms?|beats per minute)[^\d]{0,5}(\d{2,3})')
pattern_bpm_loose_fallback = BpmPattern('loose', '117 BPM”', r'\b(\d{2,3})(-|\s*)(?-i:bpms?|beats per minute)\b')

patterns_bpm = [
    pattern_bpm_range,
    pattern_bpm_appx_1,
    pattern_bpm_appx_2,
    pattern_bpm_relational,
    pattern_bpm_mention,
    pattern_bpm_loose_fallback,
]


def extract_bpm_from_name(playlist_name: pl.Expr, *, sort: bool = False):
    """"Extract a list of possible BPM specifications from the given playlist name."""
    result = pl.concat_list([
        playlist_name.str.extract_all(pat.pattern)
        for pat in patterns_bpm
    ]).list.drop_nulls().list.unique()

    return result.list.sort() if sort else result


def contains_bpm_in_name(playlist_name: pl.Expr):
    """Returns whether `playlist_name` likely contains a BPM specification."""
    return extract_bpm_from_name(playlist_name).list.len().gt(0)
