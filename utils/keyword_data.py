from typing import TypedDict

import os
import yaml

type _KeywordEntry = str | dict[str, str | None | list[_KeywordEntry]]

def _traverse_entry(entry: _KeywordEntry, tags: list[str], result: dict[str, list[str]]):
    """Visit the given `entry` and its children, and add the resulting word-to-alias mappings to `result`."""
    if isinstance(entry, str):
        result[entry] = tags if len(tags) > 1 else [*tags, entry]
    elif isinstance(entry, dict):
        for tag in entry:
            children = entry[tag]
            child_tags = [*tags, tag]
            if children is None:
                result[tag] = tags
            elif isinstance(children, str):
                result[children] = tags
            elif isinstance(children, list):
                for child in children:
                    _traverse_entry(child, child_tags, result)
    else:
        raise TypeError("Neither a str nor a dict")

class _KeywordsFile(TypedDict):
    keywords: dict[str, list[_KeywordEntry]]

def load_keyword_aliases():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/keyword_data.yaml') as stream:
        raw_data: _KeywordsFile = yaml.safe_load(stream)

    _aliases: dict[str, list[str]] = {}
    _traverse_entry(raw_data['keywords'], [], _aliases)
    return _aliases
