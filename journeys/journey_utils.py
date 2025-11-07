from typing import Callable, Literal, NotRequired, TypedDict, Unpack

from huggingface_hub import dataset_info
import polars as pl
import streamlit as st

from utils.search import SearchEngine, TagManager
from utils.tables import Playlist, PlaylistOwner, PlaylistStats, Stats, Track, TrackTag, TrackTags


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


#######################
# DATASET INFORMATION #
#######################


# TODO: Determine dataset version
dataset_name = "westie-data-collective/wcs-music-database-v1"
dataset_version = "main"


@st.cache_data(persist=True)
def dataset_link():
    """
    Link to the underlying dataset on HuggingFace,
    based on the version that was used to generate this page.
    """
    info = dataset_info(dataset_name, revision=dataset_version)
    dataset_url = f"https://huggingface.co/datasets/{info.id}"
    dataset_rev_url = f"https://huggingface.co/datasets/{info.id}/tree/{info.sha}"
    return f":blue-badge[:material/open_in_new: [{info.id} @ {info.sha[:7]} [{info.last_modified.date()}]]({dataset_rev_url})]"


def dataset_note():
    st.markdown(f"""
    > All numbers and graphs you see below are computed based on the current dataset
    {dataset_link()}, so the exact numbers you see might change between visits.
    """)


##################
# DATASET ACCESS #
##################

@st.cache_data
def load_tags_data():
    return search_engine.find_tags(limit=1000, playlist_limit=20)\
        .collect(engine='streaming')


def load_tag_manager():
    return TagManager(load_tags_data(), search_engine.data.keywords)


search_engine = SearchEngine()
search_engine.load_data()
tag_manager = load_tag_manager()

###################
# DATASET DISPLAY #
###################


base_column_config = {
    PlaylistOwner.name: st.column_config.MultiselectColumn(),
    Playlist.url: st.column_config.LinkColumn(),
    PlaylistStats.wcs_song_percent: st.column_config.ProgressColumn(format='percent'),
    Playlist.matching_song_percent: st.column_config.ProgressColumn(format='percent'),
    Stats.playlist_percent: st.column_config.ProgressColumn(format='percent'),
    Stats.song_percent: st.column_config.ProgressColumn(format='percent'),
    TrackTag.tag: tag_manager.get_column_config(TrackTag.tag),
    TrackTag.matching_playlist_count: st.column_config.NumberColumn('#'),
    TrackTag.Tag.playlist_count: st.column_config.NumberColumn('# tag'),
    TrackTag.Tag.playlist_percent: st.column_config.ProgressColumn('% tag'),
    TrackTag.Track.playlist_count: st.column_config.NumberColumn('# track'),
    TrackTag.Track.playlist_percent: st.column_config.ProgressColumn('% track'),
    TrackTags.playlist_counts_per_tag: st.column_config.LineChartColumn(y_min=0),
}


######################
# DATASET STATISTICS #
######################

@st.cache_data
def count(
    type: Literal['playlists', 'songs'] | None = None,
    *,
    with_tag: str | list[str] = [],
    without_tag: str | list[str] = [],
    without_tags: str | list[str] = [],
    with_any_tag: list[str] = [],
    with_bpm: bool = False,
):
    cnt = _count(type, with_tag=with_tag, with_any_tag=with_any_tag,
                 without_tag=without_tag, without_tags=without_tags,
                 with_bpm=with_bpm)

    if cnt is None:
        return "???"

    return f"**{cnt:,}**"


@st.cache_data
def percentage(
    type: Literal['playlists', 'songs'] | None = None,
    *,
    with_tag: str | list[str] = [],
    without_tag: str | list[str] = [],
    without_tags: str | list[str] = [],
    with_any_tag: list[str] = [],
    with_bpm: bool = False,
):
    if type == 'playlists' or type == 'songs':
        matching = _count(type, with_tag=with_tag, with_any_tag=with_any_tag,
                          without_tag=without_tag, without_tags=without_tags,
                          with_bpm=with_bpm)
        total = _count(type)
        pct = (matching / total) * 100

        if pct >= 10:
            return f"**{pct:.0f} %**"
        elif pct >= 1:
            return f"**{pct:.1f} %**"
        elif pct >= 0.1:
            return f"**{pct:.2f} %**"
        elif pct >= 0.01:
            return f"**{pct:.3f} %**"
        elif pct >= 0.001:
            return f"**{pct:.4f} %**"
        else:
            return f"**{pct:f} %**"

    return "??? %"


@st.cache_data
def _count(
    type: Literal['playlists', 'songs'] | None = None,
    *,
    with_tag: str | list[str] = [],
    without_tag: str | list[str] = [],
    without_tags: str | list[str] = [],
    with_any_tag: list[str] = [],
    with_bpm: bool = False,
):
    if type == 'playlists':
        return search_engine\
            .find_playlists(playlist_tag_include=with_tag or with_any_tag,
                            playlist_tag_exclude=without_tag or without_tags,
                            limit=None)\
            .select(Playlist.id().count())\
            .collect(engine='streaming')[Playlist.id].first()

    elif type == 'songs':
        return search_engine\
            .find_songs(tag_include=with_tag or with_any_tag,
                        tag_exclude=without_tag or without_tags,
                        song_has_bpm=True if with_bpm else None,
                        limit=None)\
            .select(Track.id().count())\
            .collect(engine='streaming')[Track.id].first()

    return None


################
# INLINE LINKS #
################


def song_link(title, artist):
    """Link to a playable version of a song (e.g. via a Spotify link)."""
    # TODO: Make song links interactive (by adding a Spotify URL and/or linking to data card)
    return f"_{title}_ by _{artist}_"


@st.cache_data
def _tagged_playlists(tag_name: str = ''):
    return search_engine\
        .find_playlists(playlist_tag_include=tag_name,
                        sort_by=[Stats.song_count],
                        limit=100)\
        .select(Playlist.name, Playlist.url, Stats.song_count)\
        .collect()


def tag_link(display_name, *, tag: str = '', code: bool = False):
    """Link to a tag."""
    tag_name = tag or display_name
    add_example("Show playlists tagged with...",
                f"**{tag_name}**",
                lambda: st.dataframe(_tagged_playlists(tag_name),
                                     column_config=base_column_config))

    # TODO: Make tag links interactive
    if code:
        return f"`{display_name}`"

    return f":green-badge[{display_name}]"


@st.cache_data
def _tagged_songs(tag_name: str = ''):
    return search_engine\
        .find_songs_by_tag(tag_name_exact=tag_name,
                           sort_by=[TrackTag.matching_playlist_count],
                           limit=100)\
        .select(Track.name, Track.artists, TrackTag.tag, TrackTag.matching_playlist_count,
                TrackTag.Tag.playlist_percent, TrackTag.Tag.playlist_count,
                TrackTag.Track.playlist_percent, TrackTag.Track.playlist_count)\
        .collect()


def song_tag_link(display_name, *, tag: str = '', code: bool = False):
    """Link to a tag."""
    tag_name = tag or display_name
    add_example("Show songs tagged with...",
                f"**{tag_name}**",
                lambda: st.dataframe(_tagged_songs(tag_name), column_config=base_column_config))

    # TODO: Make tag links interactive
    if code:
        return f"`{display_name}`"

    return f":green-badge[{display_name}]"


class SearchParameters(TypedDict):
    link_text: str
    artist_name: NotRequired[str]
    song_name: NotRequired[str]
    playlist_name: NotRequired[str]


def search_link(type: Literal['song', 'playlist'], **params: Unpack[SearchParameters]):
    # TODO: Link to main application
    pass


@st.cache_data
def _playlists_named_like(playlist_name: str):
    return search_engine\
        .find_playlists(playlist_include=playlist_name,
                        tracks_limit=10,
                        sort_by=[Playlist.matched_terms_count,
                                 Stats.song_count],
                        limit=100)\
        .select(Playlist.name, Playlist.url, Playlist.matched_terms, Stats.song_count)\
        .collect()


def playlist_name_like(playlist_name: str):
    # TODO: Render dataframe
    add_example(
        "Show playlists named...",
        f"**{playlist_name}**",
        lambda: st.dataframe(_playlists_named_like(playlist_name),
                             column_config=base_column_config))

    # TODO: Link to main application
    return f"`{playlist_name}`"
    # return search_link('playlist', link_text=playlist_name, )


def no_docstring():
    pass
