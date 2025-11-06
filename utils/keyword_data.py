"""
Classes and methods for parsing the keyword/tagging configuration in `keyword_data.yaml`.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import NamedTuple, Self, TypedDict

import os
import polars as pl
import yaml

from utils.common.dicts import append_to_entry, to_dict_of_list
from utils.common.entities import PolarsExpr, PolarsLazyFrame
from utils.tables import Tag, TrackTag

##########
# BASICS #
##########

# We're working with strings of different types

type KeywordString = str
"""Keyword string to be matched (e.g. `popular music`, `fowcs`)."""

type CategoryName = str
"""Tag category name (e.g. `genre`, `events`)."""

type ShortTagName = str
"""Short tag name without category (e.g. `pop`, `french open`)."""

type TagName = str
"""Full tag name with category (e.g. `genre:pop`, `events:french open`)."""

type HexColor = str
"""Hexadecimal color string (e.g. `#ff8800`)."""

type IconName = str
"""Icon name (e.g. `material/thumbs_up_double`)."""


def format_tag(category: CategoryName, short_name: ShortTagName) -> TagName:
    return f'{category}:{short_name}'


def format_tag_if_needed(category: CategoryName, short_name: ShortTagName) -> TagName:
    return (short_name if ':' in short_name
            else format_tag(category, short_name))


def split_tag(name: TagName) -> tuple[CategoryName, ShortTagName | None]:
    parts = name.split(':', 2)
    return (parts[0], parts[1] if len(parts) >= 2
            else parts[0], None)


def format_tag_expr(category: PolarsExpr[CategoryName], short_name: PolarsExpr[ShortTagName]) -> PolarsExpr[TagName]:
    return pl.concat_str(category, pl.lit(':'), short_name)


def split_tag_expr(tag_name: PolarsExpr[TagName]) -> pl.Expr:
    return tag_name\
        .cast(pl.String)\
        .str.splitn(':', 2)\
        .struct.rename_fields([Tag.category, Tag.short_name])


def extract_category(tag_name: PolarsExpr[TagName]) -> PolarsExpr[CategoryName]:
    return split_tag_expr(tag_name).struct.field(Tag.category)


def extract_tag(tag_name: PolarsExpr[TagName]) -> PolarsExpr[ShortTagName | None]:
    return split_tag_expr(tag_name).struct.field(Tag.short_name)


####################
# YAML FILE SCHEMA #
####################


class _KeywordsFile(TypedDict, total=False):
    """Defines the YAML schema for the `keyword_data.yaml` file."""
    colors: dict[CategoryName, HexColor]
    icons: dict[CategoryName, IconName]
    limits: dict[CategoryName, dict[ShortTagName, TagLimits]]
    hide_from_ui: TagFilterSpec
    strip_from_tracks: TagFilterSpec
    keywords: dict[CategoryName, list[KeywordOrTagSpec]]


def load_colors(keywords_file: _KeywordsFile) -> dict[CategoryName, HexColor]:
    return keywords_file.get('colors') or {}


def load_icons(keywords_file: _KeywordsFile) -> dict[CategoryName, IconName]:
    return keywords_file.get('icons') or {}


def load_aliases(keywords_file: _KeywordsFile, category_as_tag: bool = False):
    """Load the keyword-to-tag mappings."""
    _aliases: dict[KeywordString, set[TagName]] = {}
    _negated_aliases: dict[KeywordString, set[TagName]] = {}

    for category in keywords_file.get('keywords', None) or {}:
        for entry in keywords_file['keywords'][category]:
            _traverse_tag_or_keyword(entry, category,
                                     {category} if category_as_tag else set(),
                                     set(),
                                     alias_to_tags=_aliases,
                                     alias_to_negated_tags=_negated_aliases,
                                     is_negated=False,
                                     use_as_tag=True)

    return (to_dict_of_list(_aliases), to_dict_of_list(_negated_aliases))


# keywords:
#   genre:
#     - blues                      # keyword string
#     - rap:                       # tag name without explicit keywords
#     - poprock: "pop-rock"        # tag name with single keyword
#     - acoustic:                  # tag name with multiple keywords (and/or child tags)
#         - acoustic               # keyword string
#         - acoustics              # keyword string
#         - unplugged:             # child tag
#             - spotify unplugged  # keyword string
type KeywordOrTagSpec = (
    KeywordString |                # (1) keyword string
    dict[                          # (2-4) tag name with ...
        ShortTagName,              #
        None                       # (2) tag name without explicit keyword
        | str                      # (3) tag name with single keyword
        | list[KeywordOrTagSpec],  # (4) tag name with multiple keywords (and/or child tags)
    ])


def _traverse_tag_or_keyword(
    tag_or_keyword: KeywordOrTagSpec,
    parent_category: CategoryName,
    parent_tags: set[TagName],
    parent_negated_tags: set[TagName],
    is_negated: bool,
    alias_to_tags: dict[KeywordString, set[TagName]],
    alias_to_negated_tags: dict[KeywordString, set[TagName]],
    use_as_tag: bool = False
):
    """Visit the given `entry` and its children, and add the resulting keyword-to-tag mappings to `result`."""
    if isinstance(tag_or_keyword, str):
        if tag_or_keyword.startswith('+'):
            # keywords:
            #   genre:
            #     - pop:
            #         - +contemporary # <--
            for part in tag_or_keyword[1:].split('+'):
                for tag in part.split('/'):
                    parent_tags.add(format_tag_if_needed(parent_category, tag))

        elif tag_or_keyword.startswith('-'):
            # keywords:
            #   genre:
            #     - pop:
            #         - +contemporary # <--
            for part in tag_or_keyword[1:].split('+'):
                for tag in part.split('/'):
                    parent_negated_tags.add(format_tag_if_needed(parent_category, tag))

        else:
            # keywords:
            #   genre:
            #     - pop # <--
            append_to_entry(alias_to_negated_tags if is_negated else alias_to_tags,
                            tag_or_keyword.lower(), parent_tags if not use_as_tag else
                            [*parent_tags, format_tag(parent_category, tag_or_keyword)])

            if not is_negated:
                append_to_entry(alias_to_negated_tags, tag_or_keyword.lower(), parent_negated_tags)

    elif isinstance(tag_or_keyword, dict):
        negated_tags = parent_negated_tags.copy()

        # keywords:
        #   genre:
        #     - acoustic: # <--
        #       ...
        for tag_spec in tag_or_keyword:
            # Tag names can have certain modifiers
            tag = tag_spec
            is_unnamed = False
            is_lower_weight = False
            more_tags = []

            # Adding a question mark "?" to the end of a tag indicates
            # that its child entries might be only imprecise matches
            # TODO: Actually do something with that information
            if tag.endswith("?"):
                tag, is_lower_weight = tag[:-1], True
            elif tag.endswith("*"):
                tag, is_unnamed = tag[:-1], True
            elif tag.endswith("-"):
                tag, is_negated = tag[:-1], True
            else:
                tag_parts = tag.split('+')
                tag = tag_parts[0]
                more_tags = [t
                             for p in tag_parts[1:]
                             for t in p.split('/')]

            children = tag_or_keyword[tag_spec]
            child_tags = (parent_tags.copy() if is_unnamed else
                          {*parent_tags,
                           format_tag(parent_category, tag),
                           *[format_tag_if_needed(parent_category, t) for t in more_tags]})
            target = alias_to_negated_tags if is_negated else alias_to_tags

            if children is None:
                # (2) tag name without explicit keyword
                #
                # keywords:
                #   genre:
                #     - acoustic:
                append_to_entry(target, tag.lower(), child_tags)

                if not is_negated:
                    append_to_entry(alias_to_negated_tags, tag.lower(), negated_tags)

            elif isinstance(children, str):
                # (3) tag name with single keyword
                #
                # keywords:
                #   genre:
                #     - poprock: pop-rock
                append_to_entry(target, children.lower(), child_tags)

                if not is_negated:
                    append_to_entry(alias_to_negated_tags, children.lower(), negated_tags)

            elif isinstance(children, list):
                # (4) tag name with multiple keywords and/or child tags
                #
                # keywords:
                #   genre:
                #     - acoustic: # <--
                #         - acoustic
                #         - acoustics
                for child in children:
                    _traverse_tag_or_keyword(child, parent_category, child_tags, negated_tags,
                                             alias_to_tags=alias_to_tags,
                                             alias_to_negated_tags=alias_to_negated_tags,
                                             is_negated=is_negated)
            else:
                raise TypeError("Neither a str nor a list nor None")
    elif tag_or_keyword is None:
        pass
    else:
        raise TypeError(
            f"Neither a str nor a dict: {tag_or_keyword} in category {parent_category} with parent tags {parent_tags}")


class TagLimits(TypedDict, total=False):
    min_playlist_count: int
    min_playlist_percent: float


class TagLimitsMatcher(NamedTuple):
    limits_by_tag: dict[TagName, TagLimits]

    def get_limit(self, tag_name: PolarsExpr[TagName], limit_name) -> pl.Expr:
        limits = self.limits_by_tag
        limit_by_tag = {
            tag: limits[tag][limit_name]
            for tag in limits
            if limits[tag].get(limit_name)
        }
        return tag_name\
            .replace_strict(limit_by_tag, default=None)\
            .alias(f'limits.{limit_name}')

    def matches(
        self,
        tag: PolarsExpr[TagName] = TrackTag.tag(),
        playlist_count: PolarsExpr[pl.UInt32] | None = TrackTag.matching_playlist_count(),
        playlist_percent: PolarsExpr[pl.Float32] | None = TrackTag.Track.playlist_percent(),
    ) -> pl.Expr:
        min_playlist_count = self.get_limit(tag, 'min_playlist_count')
        min_playlist_percent = self.get_limit(tag, 'min_playlist_percent') / 100.0

        return pl.all_horizontal(
            min_playlist_count.pipe(lambda min: min.is_null().or_(playlist_count.ge(min)))
            if playlist_count is not None else pl.lit(True),
            min_playlist_percent.pipe(lambda min: min.is_null().or_(playlist_percent.ge(min)))
            if playlist_percent is not None else pl.lit(True))

    def filter_track_tags(self, tags: PolarsLazyFrame[TrackTag]) -> PolarsLazyFrame[TrackTag]:
        return tags.filter(self.matches())


def load_limits(keywords_file: _KeywordsFile) -> TagLimitsMatcher:
    limits = keywords_file.get('limits') or {}
    limits_by_tag = {
        format_tag(category, tag): limits[category][tag]
        for category in limits
        for tag in limits[category]
    }
    return TagLimitsMatcher(limits_by_tag)


type TagFilterSpec = dict[
    CategoryName,
    None | bool | list[ShortTagName],
]


class TagNameMatcher(NamedTuple):
    categories: list[CategoryName]
    tags: list[TagName]

    def matches(self, tag: PolarsExpr[TagName]) -> pl.Expr:
        return pl.any_horizontal(
            tag.is_in(self.tags),
            extract_category(tag).is_in(self.categories),
        )


def load_filters(keywords_file: _KeywordsFile, field_name: str) -> TagNameMatcher:
    return _parse_filter(keywords_file.get(field_name) or {})


def _parse_filter(spec: TagFilterSpec) -> TagNameMatcher:
    categories_to_filter: list[str] = []
    tags_to_filter: list[str] = []

    for category in spec:
        item = spec[category]
        if isinstance(item, bool) and item == False:
            pass
        elif item is None or (isinstance(item, bool) and item == True):
            categories_to_filter.append(category)
        elif isinstance(item, list):
            for tag in item:
                tags_to_filter.append(format_tag(category, tag))

    return TagNameMatcher(categories_to_filter, tags_to_filter)


@dataclass
class KeywordData:
    """Contains all keyword-related settings."""
    colors_by_category: dict[CategoryName, HexColor]
    icons_by_category: dict[CategoryName, IconName]
    limits_by_tag: TagLimitsMatcher
    hide_from_ui: TagNameMatcher
    strip_from_tracks: TagNameMatcher
    keywords_to_tags: dict[KeywordString, set[TagName]]
    keywords_to_excluded_tags: dict[KeywordString, set[TagName]]

    @classmethod
    def read_yaml_file(cls, file_name: str) -> KeywordData:
        """Load keyword data from a YAML file."""
        with open(file_name) as stream:
            raw_yaml: _KeywordsFile = yaml.safe_load(stream)
        return cls.read_yaml_object(raw_yaml)

    @classmethod
    def read_yaml_object(cls, keywords_file: _KeywordsFile) -> Self:
        """Load keyword data from a parsed YAML file."""
        colors = load_colors(keywords_file)
        icons = load_icons(keywords_file)
        limits = load_limits(keywords_file)
        hide_from_ui = load_filters(keywords_file, 'hide_from_ui')
        strip_from_tracks = load_filters(keywords_file, 'strip_from_tracks')
        alias_to_tags, alias_to_negated_tags = load_aliases(keywords_file, category_as_tag=False)
        return cls(colors_by_category=colors,
                   icons_by_category=icons,
                   limits_by_tag=limits,
                   hide_from_ui=hide_from_ui,
                   strip_from_tracks=strip_from_tracks,
                   keywords_to_tags=alias_to_tags,
                   keywords_to_excluded_tags=alias_to_negated_tags)


def load_keyword_data() -> KeywordData:
    """Load the default `keyword_data.yaml` file."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(dir_path, 'keyword_data.yaml')
    return KeywordData.read_yaml_file(file_path)
