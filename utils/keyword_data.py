from typing import TypedDict

import os
import yaml

type _KeywordEntry = str | dict[str, str | None | list[_KeywordEntry]]


def _append_to[Key, Value](mapping: dict[Key, set[Value]], key: Key, values: list[Value]):
    if key in mapping:
        mapping[key].update(values)
    else:
        mapping[key] = set(values)


def _format_tag(category: str, name: str) -> str:
    return f'{category}:{name}'


def _traverse_entry(entry: _KeywordEntry, category: str, tags: set[str], result: dict[str, set[str]], use_as_tag: bool = False):
    """Visit the given `entry` and its children, and add the resulting word-to-alias mappings to `result`."""
    if isinstance(entry, str):
        # keywords:
        #   genre:
        #     - pop # <--
        _append_to(result, entry, tags if not use_as_tag else
                   [*tags, _format_tag(category, entry)])
    elif isinstance(entry, dict):
        # keywords:
        #   genre:
        #     - acoustic: # <--
        #       ...
        for tag_spec in entry:
            # Tag names can have certain modifiers
            tag, is_lower_weight = tag_spec, False

            # Adding a question mark "?" to the end of a tag indicates
            # that its child entries might be only imprecise matches
            # TODO: Actually do something with that information
            if tag.endswith("?"):
                tag, is_lower_weight = tag[:-1], True

            children = entry[tag_spec]
            child_tags = [*tags, _format_tag(category, tag)]
            if children is None:
                # keywords:
                #   genre:
                #     - acoustic:
                _append_to(result, tag, tags)
            elif isinstance(children, str):
                # keywords:
                #   genre:
                #     - poprock: pop-rock
                _append_to(result, children, tags)
            elif isinstance(children, list):
                # keywords:
                #   genre:
                #     - acoustic: # <--
                #         - acoustic
                #         - acoustics
                for child in children:
                    _traverse_entry(child, category, child_tags, result)
            else:
                raise TypeError("Neither a str nor a list nor None")
    else:
        raise TypeError("Neither a str nor a dict")


class _KeywordsFile(TypedDict):
    colors: dict[str, str]
    keywords: dict[str, list[_KeywordEntry]]


def load_keyword_aliases(category_as_tag: bool = False):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        raw_data: _KeywordsFile = yaml.safe_load(stream)

    _aliases: dict[str, set[str]] = {}
    for category in raw_data['keywords']:
        for entry in raw_data['keywords'][category]:
            _traverse_entry(entry, category, [category] if category_as_tag else [], _aliases, True)

    return {k: list(_aliases[k]) for k in _aliases}


def load_keyword_colors():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        raw_data: _KeywordsFile = yaml.safe_load(stream)

    return raw_data['colors']
