from typing import Iterable


def append_to_entry[Key, Value](mapping: dict[Key, set[Value]], key: Key, values: list[Value]):
    if key in mapping:
        mapping[key].update(values)
    else:
        mapping[key] = set(values)


def to_dict_of_list[Key, Value](mapping: dict[Key, Iterable[Value]]) -> dict[Key, list[Value]]:
    return {k: list(mapping[k]) for k in mapping}


def to_dict_of_set[Key, Value](mapping: dict[Key, Iterable[Value]]) -> dict[Key, set[Value]]:
    return {k: set(mapping[k]) for k in mapping}
