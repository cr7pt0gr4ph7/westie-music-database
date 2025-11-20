"""
Classes and methods for parsing the keyword/tagging configuration in `keyword_data.yaml`.
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import StrEnum
from itertools import combinations, product
from typing import Literal, NamedTuple, Self, TypedDict

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
    parts = name.split(':', 1)
    return ((parts[0], parts[1]) if len(parts) >= 2
            else (parts[0], None))


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

class _CategoryMetadata(TypedDict, total=False):
    icon: IconName
    color: HexColor
    hide_from_ui: bool | list[ShortTagName]
    strip_from_tracks: bool | list[ShortTagName]


class _KeywordsFile(TypedDict, total=False):
    """Defines the YAML schema for the `keyword_data.yaml` file."""
    metadata: dict[CategoryName, _CategoryMetadata]
    limits: dict[CategoryName, dict[ShortTagName, TagLimits]]
    relations: list[dict[Literal['opposites', 'related'], list[TagName | dict[Literal['group'], list[TagName]]]]]
    keywords: dict[CategoryName, list[KeywordOrTagSpec]]


def load_colors(keywords_file: _KeywordsFile) -> dict[CategoryName, HexColor]:
    metadata = keywords_file.get('metadata') or {}
    return {category: metadata[category].get('color')
            for category in metadata
            if 'color' in metadata[category]}


def load_icons(keywords_file: _KeywordsFile) -> dict[CategoryName, IconName]:
    metadata = keywords_file.get('metadata') or {}
    return {category: metadata[category].get('icon')
            for category in metadata
            if 'icon' in metadata[category]}


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
    metadata = keywords_file.get('metadata') or {}
    filter_spec = {category: metadata[category].get(field_name)
                   for category in metadata
                   if field_name in metadata[category]}
    return _parse_filter(filter_spec)


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


class TagRelationType(StrEnum):
    OPPOSITES = 'opposites'
    RELATED = 'related'

    @staticmethod
    def from_string(type: str) -> TagRelationType:
        if type == 'opposites':
            return TagRelationType.OPPOSITES
        elif type == 'related':
            return TagRelationType.RELATED
        else:
            raise ValueError(f"Invalid tag relation type: {type}")


@dataclass
class TagRelation:
    type: TagRelationType
    tags: list[TagName]

    @property
    def name(self) -> str:
        match self.type:
            case TagRelationType.OPPOSITES:
                return " ~ ".join(self.tags)

            case TagRelationType.RELATED:
                return " vs. ".join(self.tags)

            case _:
                raise ValueError(f"Invalid TagRelationType value: {self.type}")


@dataclass
class TagRelations:
    relations: list[TagRelation]

    def of_type(self, type: TagRelationType) -> list[TagRelation]:
        return [rel for rel in self.relations
                if rel.type == type]

    def get_relations(self, tag_name: TagName, type: TagRelationType | None = None) -> list[TagRelation]:
        return [rel for rel in self.relations
                if type is None or rel.type == type
                if tag_name in rel.tags]

    def get_opposite_tags(self, tag_name: TagName) -> set[TagName]:
        return {tag for rel in self.relations
                if rel.type == TagRelationType.OPPOSITES
                if tag_name in rel.tags
                for tag in rel.tags
                if tag != tag_name}

    def get_related_tags(self, tag_name: TagName) -> set[TagName]:
        return {tag for rel in self.relations
                if rel.type == TagRelationType.RELATED
                if tag_name in rel.tags
                for tag in rel.tags
                if tag != tag_name}


def load_relations(keywords_file: _KeywordsFile) -> TagRelations:
    def get_relation_item_sets(relation: list[TagName | dict[Literal['group'], list[TagName]]]) -> list[list[TagName]]:
        groups: list[list[TagName]] = []
        standalone: list[TagName] = []

        for item in relation:
            if isinstance(item, dict):
                groups.append(item['group'])
            else:
                standalone.append(item)

        return list([[*standalone, *items] for items in product(*groups)] if groups else [standalone])

    return TagRelations([
        TagRelation(TagRelationType.from_string(type), item_set)
        for relation in keywords_file.get('relations') or []
        for type in relation
        for item_set in get_relation_item_sets(relation[type])
    ])


@dataclass
class KeywordData:
    """Contains all keyword-related settings."""
    colors_by_category: dict[CategoryName, HexColor]
    icons_by_category: dict[CategoryName, IconName]
    limits_by_tag: TagLimitsMatcher
    hide_from_ui: TagNameMatcher
    strip_from_tracks: TagNameMatcher
    relations: TagRelations
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
        relations = load_relations(keywords_file)
        alias_to_tags, alias_to_negated_tags = load_aliases(keywords_file, category_as_tag=False)
        return cls(colors_by_category=colors,
                   icons_by_category=icons,
                   limits_by_tag=limits,
                   hide_from_ui=hide_from_ui,
                   strip_from_tracks=strip_from_tracks,
                   relations=relations,
                   keywords_to_tags=alias_to_tags,
                   keywords_to_excluded_tags=alias_to_negated_tags)


def load_keyword_data() -> KeywordData:
    """Load the default `keyword_data.yaml` file."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(dir_path, 'keyword_data.yaml')
    return KeywordData.read_yaml_file(file_path)
