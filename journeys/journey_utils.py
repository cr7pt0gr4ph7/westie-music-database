from typing import Callable

import streamlit as st


####################
# EXAMPLE SECTIONS #
####################


def sample_section(title: str, *, expanded: bool = False):
    return st.expander(f":blue-badge[Sample] **{title}**", expanded=expanded)


def _render_example_section(groups: dict[str, list[tuple[str, Callable[[], None]]]]):
    columns = st.columns(len(groups))

    is_first = True
    index = 0
    for group in groups:
        w = groups[group]

        with columns[index]:
            with st.container(border=True, height='stretch'):
                selected_index = st.pills(
                    group,
                    range(len(w)),
                    format_func=lambda i: w[i][0],
                    default=None,
                    # label_visibility='collapsed',
                )

                if selected_index is not None:
                    selected_widget = w[selected_index][1]
                    selected_widget()

        is_first = False
        index += 1


class InlineExamplesContainer:
    examples: dict[str, list[tuple[str, Callable[[], None]]]]

    def __init__(self):
        self.examples = {}

    def add_example(self, group: str, title: str, callback: Callable[[], None]):
        self.examples.setdefault(group, []).append((title, callback))

    def pop_examples(self):
        result = self.examples
        self.examples = {}
        return result

    def render_example_section(self, name: str):
        groups = self.pop_examples()

        if len(groups) == 0:
            return

        def fragment():
            _render_example_section(groups)

        fragment.__qualname__ = name
        fragment.__name__ = name

        st.fragment(fragment)()


global_examples = InlineExamplesContainer()
add_example = global_examples.add_example
render_examples = global_examples.render_example_section
