from typing import TypedDict

import os
import yaml

from utils.common.dicts import append_to_entry, to_dict_of_list

type _KeywordEntry = str | dict[str, str | None | list[_KeywordEntry]]


class _KeywordsFile(TypedDict):
    colors: dict[str, str]
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
        raise TypeError(f"Neither a str nor a dict")


def load_keyword_aliases(category_as_tag: bool = False):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        raw_data: _KeywordsFile = yaml.safe_load(stream)

    _aliases: dict[str, set[str]] = {}
    _negated_aliases: dict[str, set[str]] = {}

    for category in raw_data['keywords']:
        for entry in raw_data['keywords'][category]:
            _traverse_entry(entry, category,
                            [category] if category_as_tag else [],
                            alias_to_tags=_aliases,
                            alias_to_negated_tags=_negated_aliases,
                            is_negated=False,
                            use_as_tag=True)

    return (to_dict_of_list(_aliases), to_dict_of_list(_negated_aliases))


def load_keyword_colors():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        raw_data: _KeywordsFile = yaml.safe_load(stream)

    return raw_data['colors']
