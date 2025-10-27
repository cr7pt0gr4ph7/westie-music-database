from typing import Final, NamedTuple, TypedDict

import os
import polars as pl
import yaml

from utils.common.dicts import append_to_entry, to_dict_of_list
from utils.tables import Tag, TrackTag

type _KeywordEntry = str | dict[str, str | None | list[_KeywordEntry]]

type _FilterSpec = dict[str, None | bool | list[str]]


class _LimitsSpec(TypedDict):
    min_playlist_count: int
    min_playlist_percent: float


class _KeywordsFile(TypedDict):
    colors: dict[str, str]
    limits: dict[str, dict[str, _LimitsSpec]]
    strip_from_tracks: _FilterSpec
    keywords: dict[str, list[_KeywordEntry]]


def _format_tag(category: str, name: str) -> str:
    return f'{category}:{name}'


def _traverse_entry(
    entry: _KeywordEntry, category: str, tags: set[str],
    is_negated: True,
    alias_to_tags: dict[str, set[str]],
    alias_to_negated_tags: dict[str, set[str]],
    use_as_tag: bool = False
):
    """Visit the given `entry` and its children, and add the resulting word-to-alias mappings to `result`."""
    if isinstance(entry, str):
        # keywords:
        #   genre:
        #     - pop # <--
        append_to_entry(alias_to_negated_tags if is_negated else alias_to_tags,
                        entry, tags if not use_as_tag else
                        [*tags, _format_tag(category, entry)])

    elif isinstance(entry, dict):
        # keywords:
        #   genre:
        #     - acoustic: # <--
        #       ...
        for tag_spec in entry:
            # Tag names can have certain modifiers
            tag, is_lower_weight, is_negated = tag_spec, False, False

            # Adding a question mark "?" to the end of a tag indicates
            # that its child entries might be only imprecise matches
            # TODO: Actually do something with that information
            if tag.endswith("?"):
                tag, is_lower_weight = tag[:-1], True
            elif tag.endswith("-"):
                tag, is_negated = tag[:-1], True

            children = entry[tag_spec]
            child_tags = [*tags, _format_tag(category, tag)]
            target = alias_to_negated_tags if is_negated else alias_to_tags

            if children is None:
                # keywords:
                #   genre:
                #     - acoustic:
                append_to_entry(target, tag, tags)

            elif isinstance(children, str):
                # keywords:
                #   genre:
                #     - poprock: pop-rock
                append_to_entry(target, children, tags)

            elif isinstance(children, list):
                # keywords:
                #   genre:
                #     - acoustic: # <--
                #         - acoustic
                #         - acoustics
                for child in children:
                    _traverse_entry(child, category, child_tags,
                                    alias_to_tags=alias_to_tags,
                                    alias_to_negated_tags=alias_to_negated_tags,
                                    is_negated=is_negated)
            else:
                raise TypeError("Neither a str nor a list nor None")
    else:
        raise TypeError(f"Neither a str nor a dict: {entry} in category {category} with parent tags {tags}")


def _load_keyword_data_from_yaml() -> _KeywordsFile:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        return yaml.safe_load(stream)


def load_keyword_aliases(category_as_tag: bool = False):
    _aliases: dict[str, set[str]] = {}
    _negated_aliases: dict[str, set[str]] = {}
    raw_data = _load_keyword_data_from_yaml()

    for category in raw_data.get('keywords', None) or {}:
        for entry in raw_data['keywords'][category]:
            _traverse_entry(entry, category,
                            [category] if category_as_tag else [],
                            alias_to_tags=_aliases,
                            alias_to_negated_tags=_negated_aliases,
                            is_negated=False,
                            use_as_tag=True)

    return (to_dict_of_list(_aliases), to_dict_of_list(_negated_aliases))


def load_keyword_limits():
    raw_data = _load_keyword_data_from_yaml()
    limits = raw_data.get('limits') or {}
    result: dict[str, _FilterSpec] = {}

    for category in limits:
        tags = limits[category]
        for tag in tags:
            result[_format_tag(category, tag)] = tags[tag]

    return result


def filter_track_tags_by_limits(tags: pl.LazyFrame, limits: dict[str, _LimitsSpec]) -> pl.LazyFrame:
    def get_tag_limits(limit_name):
        return {tag: limits[tag][limit_name] for tag in limits if limits[tag].get(limit_name)}

    TEMP_MIN_PLAYLIST_COUNT: Final = 'temp_min_playlist_count'
    TEMP_MIN_PLAYLIST_PERCENT: Final = 'temp_min_playlist_percent'

    return tags\
        .with_columns(pl.col(TrackTag.tag).replace_strict(get_tag_limits('min_playlist_count'), default=None)
                      .alias(TEMP_MIN_PLAYLIST_COUNT),
                      pl.col(TrackTag.tag).replace_strict(get_tag_limits('min_playlist_percent'), default=None)
                      .alias(TEMP_MIN_PLAYLIST_PERCENT))\
        .filter(pl.col(TEMP_MIN_PLAYLIST_COUNT).is_null()
                .or_(TrackTag.matching_playlist_count().ge(pl.col(TEMP_MIN_PLAYLIST_COUNT))),
                pl.col(TEMP_MIN_PLAYLIST_PERCENT).is_null()
                .or_(TrackTag.Track.playlist_percent().ge(pl.col(TEMP_MIN_PLAYLIST_PERCENT) / 100)))\
        .drop(TEMP_MIN_PLAYLIST_COUNT, TEMP_MIN_PLAYLIST_PERCENT)


class TagsToFilter(NamedTuple):
    categories: list[str]
    tags: list[str]


def _parse_keyword_filter(spec: _FilterSpec) -> TagsToFilter:
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
                tags_to_filter.append(_format_tag(category, tag))

    return TagsToFilter(categories_to_filter, tags_to_filter)


def load_track_keyword_filter() -> tuple[list[str], list[str]]:
    raw_data = _load_keyword_data_from_yaml()
    return _parse_keyword_filter(raw_data.get('strip_from_tracks') or {})


def tag_matches_filter(filter: TagsToFilter, tag: pl.Expr) -> pl.Expr:
    return pl.any_horizontal(
        tag.is_in(filter.tags),
        tag.str.split(':').list.get(0).is_in(filter.categories),
    )


def load_keyword_colors():
    raw_data = _load_keyword_data_from_yaml()
    return raw_data.get('colors') or {}
