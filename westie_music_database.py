from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from threading import RLock
from typing import Final, LiteralString

import altair as alt
import streamlit as st
import wordcloud
import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import psutil
import time

from utils.common.entities import Field
from utils.common.logging import log_query
from utils.common.polars import is_in_range
from utils.keyword_data import split_tag
from utils.playlist_classifiers import contains_bpm_in_name
from utils.pull_data import automatically_pull_data_if_needed
from utils.search import SearchEngine, TagManager
from utils.spotify import create_spotify_client, create_spotify_playlist, is_spotify_integration_configured, spotify_login_button
from utils.tables import Playlist, PlaylistOwner, PlaylistStats, PlaylistTags, PlaylistTrack, Stats, Tag, TagsData, Track, TrackAdjacent, TrackLyrics, TrackTag, TrackTags

# As mentioned in the streamlit docs pyplot doesn't work well with threads,
# so use a lock to protect it (as recommeded by the streamlit documentation)
# See: https://docs.streamlit.io/develop/api-reference/charts/st.pyplot
_lock = RLock()

# avail_threads = pl.threadpool_size()
pl.Config.set_tbl_rows(100).set_fmt_str_lengths(100)
pl.enable_string_cache()  # for Categoricals
# st.text(f"{avail_threads}")

github_branch = "main"
github_repo = "cr7pt0gr4ph7/westie-music-database"
keyword_file_url = f"https://github.com/{github_repo}/blob/{github_branch}/utils/keyword_data.yaml"

# Only check once per session
if "pull_data" not in st.session_state or st.session_state["pull_data"]:
    # Automatically pull the data from HuggingFace if we're running on
    # Streamlit Community Cloud, as it doesn't seem to provide a separate
    # customizable setup step.
    #
    # This step does nothing when run in a local environment.
    automatically_pull_data_if_needed()
    st.session_state["pull_data"] = False


def immediate[R](func: Callable[[], R]) -> R:
    return func()


def just_a_peek(df_):
    '''just peeks at the df where it is'''
    st.write(df_.schema)
    return df_


def gen(iterable):
    '''converts iterable item to generator to save on memory'''
    for _ in iterable:
        yield _


def sample_with_bpm_range(df, prev_bpm):
    '''Helper function to sample song with 5–8 bpm diff for playlist generator'''
    return df.filter(
        (pl.col("bpm") - prev_bpm).abs().is_between(5, 8)
    ).sample(n=1, seed=42)


def determine_column_config(field: Field[LiteralString, pl.DataType]):
    name = field.field_name
    docs = field.field_docs
    dtype = field.field_type

    is_url = name.endswith("_url") or name.endswith(".url") or name == "url"
    is_percent = name.endswith("_percent") or name.endswith(".percent") or field.field_name == "percent"
    is_text = dtype is not None and dtype.is_(pl.String)

    if name and docs:
        # Escape special markdown characters
        escaped_name = name.replace("_", "\\_")
        docs = f"**{escaped_name}**:  \n{docs}"

    if is_url:
        return st.column_config.LinkColumn(
            help=docs,
            display_text=r"https://(open\.spotify\.com/.*)")
    elif is_percent:
        return st.column_config.ProgressColumn(
            help=docs,
            format="percent",
            min_value=0.0,
            max_value=1.0)
    elif is_text:
        return st.column_config.TextColumn(
            help=docs)
    else:
        return st.column_config.Column(
            help=docs)


def determine_column_configs(fields: list[Field[LiteralString, pl.DataType]]):
    return {field: determine_column_config(field) for field in fields}


link_columns = determine_column_configs([
    Playlist.url,
    PlaylistOwner.url,
    Track.url,
])

track_columns = link_columns | determine_column_configs([
    Track.id,
    Track.name,
    Track.artists,
    Track.artist_names,
    Track.has_queer_artist,
    Track.has_poc_artist,
    Track.beats_per_minute,
    Track.release_date,
    Stats.playlist_count.with_help(help="# of playlists which contain the song"),
    Stats.dj_count.with_help(help="# of DJs that have this song in their playlists"),
    Track.country,
    Track.region,
    TrackAdjacent.times_played_together,
    TrackLyrics.lyrics,
    TrackLyrics.matched_lyrics,
    TrackLyrics.matched_lyrics_count,
])

playlist_columns = link_columns | determine_column_configs([
    Playlist.matching_song_count,
    Playlist.matching_song_percent,
    Playlist.matched_terms,
    PlaylistStats.wcs_song_count,
    PlaylistStats.wcs_song_percent,
    PlaylistStats.total_song_count,
    Playlist.name,
    Playlist.extracted_dates,
    Playlist.is_social_set,
    Playlist.country,
    Playlist.region,
])

tag_columns = determine_column_configs([
    Tag.playlist_count,
    Tag.song_count,
    Tag.max_playlist_count,
    Playlist.name,
])

dj_columns = link_columns | determine_column_configs([
    PlaylistOwner.id,
    PlaylistOwner.is_wcs_dj,
    PlaylistOwner.name,
    PlaylistOwner.country,
    PlaylistOwner.region,
])


def load_search_engine():
    engine = SearchEngine()
    engine.load_data()
    return engine


def wcs_specific(df_: pl.DataFrame):
    """Given a LazyFrame, filter to the records most likely to be West Coast Swing related"""
    return (df_.lazy()
            .filter(pl.col(Playlist.is_social_set).eq(True)
                    | pl.col(PlaylistOwner.is_wcs_dj).eq(True)
                    | pl.col(Playlist.name).cast(pl.String).str.contains_any(['wcs', 'social', 'party', 'soirée', 'west', 'routine',
                                                                              'practice', 'practise', 'westie', 'party', 'beginner',
                                                                              'bpm', 'swing', 'novice', 'intermediate', 'comp',
                                                                              'musicality', 'timing', 'pro show'], ascii_case_insensitive=True))
            )


# makes it so streamlit doesn't have to reload for every sesson.
@st.cache_resource
def load_notes():
    return (pl.scan_csv('unprocessed_data_huggingface/data_notes.csv')
            .rename({'Artist': Track.artist_names, 'Song': Track.name})
            .with_columns(pl.col([Track.name, Track.artist_names]).cast(pl.Categorical))
            )


@st.cache_data
def load_countries():
    return search_engine.data.countries


@st.cache_data
def load_stats():
    return search_engine.get_stats()


@st.cache_data
def load_tags_data():
    return search_engine.find_tags(limit=1000, playlist_limit=20)\
        .collect(engine='streaming')


@st.cache_resource
def load_tag_manager():
    return TagManager(load_tags_data(), search_engine.data.keywords)


search_engine = load_search_engine()
df_notes = load_notes()
countries = load_countries()
songs_count, artists_count, playlists_count, djs_count, lyrics_count = load_stats()
tag_manager = load_tag_manager()
spotify_client = create_spotify_client()  # may be None if not logged in


# st.write(f"Memory Usage: {psutil.virtual_memory().percent}%")
st.markdown("## Westie Music Database:")
# byebye memory problems courtesy of Lukas W
st.text("An aggregated collection of West Coast Swing (WCS) music and playlists from DJs, Spotify users, etc. ")

st.write(f"{songs_count:,}   Songs")
st.write(f"{artists_count:,}   Artists")
st.write(f"{playlists_count:,}   Playlists")
st.write(f"{djs_count:,}   Westies/DJs\n\n")

st.link_button("Help fill in country info!",
               url='https://docs.google.com/spreadsheets/d/1YQaWwtIy9bqSNTXR9GrEy86Ix51cvon9zzHVh7sBi0A/edit?usp=sharing')


feature_flags: dict[str, (bool, str)] = {}


def feature_flag(name: str, default: bool = False, help: str = "") -> bool:
    flag_name = f"feature.{name}"
    result = default or st.query_params.get(flag_name) in ["1", "yes", "on", "true"]
    feature_flags[flag_name] = (result, help)
    return result


enable_random_song = feature_flag(
    'random_song',
    help="Enable the \"Random Song\" section.")

enable_song_distance = feature_flag(
    'song_distance',
    help="Enable the \"Song distance\" section.")

enable_show_containing_playlists = feature_flag(
    'show_containing_playlists',
    help="List containing playlists in the \"Find a Song\" section.")

enable_show_playlist_keywords = feature_flag(
    'show_playlist_keywords',
    help="List common keywords in the \"Find a Playlist\" section.")

enable_show_related_tags = feature_flag(
    'show_related_tags',
    help="Show possibly related tags in the \"Explore Songs by Tags\" section.")

enable_spotify_integration = feature_flag(
    'spotify_login',
    help="Enable Spotify integration features.",
)

if enable_spotify_integration and is_spotify_integration_configured():
    st.space("small")

    with st.container():
        st.markdown("You can optionally log in with your Spotify account to generate playlists based on your search results:")
        spotify_login_button()

    st.space("small")
else:
    st.markdown("#### ")

if "experimental" in st.query_params:
    # Shortcut to enable ALL feature flags at once
    if st.query_params["experimental"] == "all":
        for flag in feature_flags:
            st.query_params[flag] = "1"

        st.query_params["experimental"] = ""
        st.rerun()

    st.markdown(
        """
        ####
        #### Feature flags for Developers 🚧
        **Use at your own risk!**
        """)

    new_feature_flags = st.data_editor([
        {"name": flag, "value": feature_flags[flag][0], "help": feature_flags[flag][1]}
        for flag in feature_flags
    ], num_rows="fixed", disabled=[
        "name",
        "help",
    ], column_order=[
        "name",
        "help",
        "value",
    ], column_config={
        "name": st.column_config.TextColumn("Feature Flag"),
        "value": st.column_config.CheckboxColumn("Enabled"),
        "help": st.column_config.TextColumn("Description"),
    })

    with st.container(horizontal=True):
        if st.button("[Hide Developer Options]", type='tertiary',
                     help="Hide this table and remove the `?experimental` parameter from the URL."):
            del st.query_params["experimental"]
            st.rerun()

        if st.button("[Clear]", type='tertiary',
                     help="Reset all feature flags back to their defaults."):
            st.query_params.from_dict({"experimental": ""})
            st.rerun()

    flags_changed = False
    for row in new_feature_flags:
        flag = row["name"]
        if (old_value := feature_flags[flag][0]) != (new_value := row["value"]):
            flags_changed = True
            if new_value:
                st.query_params[flag] = "1"
            else:
                del st.query_params[flag]

    if flags_changed:
        st.rerun()


if enable_random_song:
    @immediate
    @st.fragment
    def section_random_song():
        with st.container(horizontal=True):
            st.markdown(f"#### Random Song")
            st.button(":material/refresh:")

        random_song = search_engine\
            .find_random_songs(playlist_count_range=(50, None),
                               dj_count_range=(20, None),
                               limit=1)\
            .with_columns(TrackTags.extract_tags(),
                          TagsData(TrackTags.tags_data()).compute_confidence_scores().tags_data())\
            .collect(engine="streaming")

        st.dataframe(random_song.select(Track.name, Track.artists, Track.url),
                     column_config=track_columns)

        with st.container(border=True):
            track_title = random_song[Track.name].item()
            track_artists = random_song[Track.artist_names].item()
            track_url = random_song[Track.url].item()
            track_tags = random_song[TrackTags.tags_data].item()

            st.markdown(":red-badge[:material/info: About]")

            st.metric(label=f"Track \u2e3a [Open in Spotify]({track_url}) [:material/open_in_new:]({track_url})",
                      value=track_title + " \u2014 " + track_artists)

            with st.container(horizontal=True):
                st.metric(label="Playlists",
                          value=random_song[Stats.playlist_count].item())

                st.metric(label="DJs",
                          value=random_song[Stats.dj_count].item())

            if track_tags.to_list():
                st.divider(width=200)

                st.markdown(":red-badge[:material/sell: Top Tags]")

                with st.container(horizontal=True):
                    for tag in track_tags:
                        category = split_tag(tag[TrackTag.tag])[0]
                        if tag[TrackTag.confidence] < 0.4 and category != "topic":
                            continue

                        st.metric(label=tag_manager.format_category(category) + " :material/sell:",
                                  value=split_tag(tag[TrackTag.tag])[-1],
                                  delta=tag[TrackTag.confidence])
        st.markdown(f"#### ")


# @st.cache_data
# def sample_of_raw_data():
#     return (df
#             # .with_columns(pl.col(Track.artist_names).cast(pl.String))
#             .join(pl.scan_parquet('processed_data/data_song_bpm.parquet')
#                   .with_columns(pl.col([Track.name, Track.artist_names]).cast(pl.Categorical)),
#                   how='left', on=[Track.name, Track.artist_names])
#             # .with_columns(pl.col(Track.artist_names).cast(pl.Categorical))
#             .head(100000).collect().sample(500)
#             )


# sample_of_raw_data = sample_of_raw_data()

# data_view_toggle = st.toggle("📊 Raw data")

# if data_view_toggle:
#     # num_records = st.slider("How many records?", 1, 1000, step=50)
#     st.dataframe(sample_of_raw_data,
#                  column_config=link_columns)
#     st.markdown(f"#### ")


st.markdown("#### Choose your own adventure!")

# TODO: For general usage, it would be best to pre-compute the "Top Song"
#       lists at build time


@st.cache_data
def top_songs():
    """Returns the top songs aggregated over all playlists."""
    return search_engine\
        .find_songs(
            sort_by=Stats.playlist_count,
            descending=True,
            limit=101
        )\
        .rename({Track.country: 'country', Track.region: 'region'})\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .with_row_index(offset=1)\
        .collect(engine='streaming')


@st.cache_data
def top_queer_songs():
    """Returns the top songs by queer artists aggregated over all playlists."""
    return search_engine\
        .find_songs(
            artist_is_queer=True,
            sort_by=Stats.playlist_count,
            descending=True,
            limit=100,
        )\
        .rename({Track.country: 'country', Track.region: 'region'})\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .with_row_index(offset=1)\
        .collect(engine='streaming')


@st.cache_data
def top_poc_songs():
    """Returns the top songs by POC artists aggregated over all playlists."""
    return search_engine\
        .find_songs(
            artist_is_poc=True,
            sort_by=Stats.playlist_count,
            descending=True,
            limit=100,
        )\
        .rename({Track.country: 'country', Track.region: 'region'})\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .with_row_index(offset=1)\
        .collect(engine='streaming')


@immediate
@st.fragment
def section_top_songs():
    top_songs_toggle = st.toggle("Top Songs")
    if not top_songs_toggle:
        return

    top_song_columns = [
        Track.name,
        Track.url,
        Stats.playlist_count,
        Stats.dj_count,
        Track.beats_per_minute,
        Track.has_queer_artist,
        Track.has_poc_artist,
        Playlist.name,
        Track.artists,
        PlaylistOwner.name,
        'country',
    ]

    top_songs_df = top_songs()
    st.markdown(f"Top 100 WCS songs!")
    st.link_button('Playlist of the top 100',
                   url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_songs_df.drop(Stats.playlist_count),
                 column_order=top_song_columns, column_config=track_columns)

    st.markdown("Top 100 🏳️‍🌈 songs!")
    top_queer_songs_df = top_queer_songs()

    # st.link_button('Playlist of the top 100',
    #        url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_queer_songs_df.drop(Stats.playlist_count),
                 column_order=top_song_columns, column_config=track_columns)

    st.markdown("Top 100 POC songs!")
    top_poc_songs_df = top_poc_songs()

    # st.link_button('Playlist of the top 100',
    #        url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_poc_songs_df.drop(Stats.playlist_count),
                 column_order=top_song_columns, column_config=track_columns)


@immediate
@st.fragment
def section_find_song():
    # Courtesy of Vishal S.
    song_locator_toggle = st.toggle("Find a Song 🎵")
    if not song_locator_toggle:
        return

    song_col1, song_col2 = st.columns(2)
    with song_col1:
        song_input = st.text_input("Song name:")
        artist_name = st.text_input("Artist name:")
        dj_input = st.text_input("DJ/user name:")
        playlist_input = st.text_input(
            "Playlist name ('late night', '80bpm', or 'Budafest'):")
        queer_toggle = st.checkbox("🏳️‍🌈")
        poc_toggle = st.checkbox("POC")
        st.markdown(
            "[Add/correct POC artists](https://docs.google.com/spreadsheets/d/1-elrLd_3tX4QTLQjj4EmPxRSzXHcxs6tZp5Y5fRFalc/edit?usp=sharing)")

    with song_col2:
        countries_selectbox = st.multiselect("Country:", countries)
        added_2_playlist_date = st.text_input(
            "Added to playlist date (yyyy-mm-dd):")
        track_release_date = st.text_input(
            "Track release date (yyyy-mm-dd or '198' for 1980's music):")
        anti_playlist_input = st.text_input(
            "Exclude if in playlists ('blues', or 'zouk'):")
        num_results = st.number_input(
            "Skip the top __ results", value=0, min_value=0, step=250)
        # num_results = st.slider("Skip the top __ results", 0, 111000, step=500)
        bpm_slider = st.slider("Search BPM:", 0, 250, (0, 250))

    if not countries_selectbox:
        countries_2_filter = countries
    if countries_selectbox:
        countries_2_filter = countries_selectbox

    with st.container(border=True):
        st.markdown(":small[**Settings for high/low playlist generator**]")

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            bpm_low = st.number_input(
                "Low BPM: ", value=90, min_value=0, step=2)
        with col2:
            bpm_med = st.number_input(
                "Medium BPM: ", value=95, min_value=0, step=2)
        with col3:
            bpm_high = st.number_input(
                "High BPM: ", value=100, min_value=0, step=2)


    with st.container(horizontal=True, horizontal_alignment='left'):
        perform_search = st.button("Search songs",  type="primary")
        create_playlist = (enable_spotify_integration
                           and spotify_client is not None
                           and st.button(":material/playlist_add: Create Spotify Playlist!",
                                         help="Create a Spotify playlist based on the search results."))

    if perform_search or create_playlist:
        log_query("Search songs", {'song_input': song_input,
                                   'artist_name': artist_name,
                                   'dj_input': dj_input,
                                   'playlist_input': playlist_input,
                                   'queer_toggle': queer_toggle,
                                   'poc_toggle': poc_toggle,
                                   'countries_selectbox': countries_selectbox,
                                   'added_2_playlist_date': added_2_playlist_date,
                                   'track_release_date': track_release_date,
                                   'anti_playlist_input': anti_playlist_input,
                                   'num_results': num_results,
                                   'bpm_slider': bpm_slider,
                                   }
                  )

        song_search_df = search_engine.find_songs(
            song_name=song_input,
            song_bpm_range=bpm_slider,
            artist_name=artist_name,
            artist_is_queer=queer_toggle,
            artist_is_poc=poc_toggle,
            playlist_include=playlist_input,
            playlist_exclude=anti_playlist_input,
            added_to_playlist_date=added_2_playlist_date,
            skip_num_top_results=num_results,
            sort_by=[
                Playlist.matched_terms_count,
                Playlist.matching_playlist_count,
                Stats.playlist_count,
                Stats.dj_count
            ],
            descending=True,
            limit=100,
        ).collect(engine="streaming")

        if create_playlist:
            playlist_query_text = "Tracks"
            if song_input:
                playlist_query_text += f" named \"{song_input}\""
            if artist_name or queer_toggle or poc_toggle:
                playlist_query_text += " by"
                if queer_toggle or poc_toggle:
                    if queer_toggle and poc_toggle:
                        playlist_query_text += " Queer & POC artists"
                    elif queer_toggle:
                        playlist_query_text += " Queer artists"
                    elif poc_toggle:
                        playlist_query_text += " POC artists"
                    if artist_name:
                        playlist_query_text += " named "
                if artist_name:
                    playlist_query_text += f" \"{artist_name}\""
            if playlist_input:
                playlist_query_text += f" occuring in \"{playlist_input}\" playlists"
            if playlist_input and anti_playlist_input:
                playlist_query_text += f" but"
            if anti_playlist_input:
                playlist_query_text += f" not occuring in \"{playlist_input}\" playlists"
            if added_2_playlist_date:
                playlist_query_text += f" added {added_2_playlist_date}"

            playlist_url = create_spotify_playlist(
                spotify_client,
                name = f"🔍 {playlist_query_text} (from Westie Music Database)",
                description = "This playlist was generated using https://wcs-music-database.streamlit.app 🪄",
                tracks=song_search_df,
            )

            st.markdown(f"Find your new playlist here: {playlist_url}")

        results_df = song_search_df.lazy()\
            .with_columns(
                pl.col(Playlist.name).list.head(30),
                (
                    TagsData(TrackTags.tags_data())
                    .filter(category=["genre", "mood", "tempo", "level", "topic"])
                    .sort_by_frequency()
                    .tags_with_frequencies()
                    .alias(TrackTags.tag_frequency)
                ),
                (
                    TagsData(TrackTags.tags_data())
                    .filter(category=["genre", "mood", "tempo", "level", "topic"])
                    .sort_by_frequency()
                    .playlist_counts_per_tag()
                    .alias(TrackTags.playlist_counts_per_tag)
                ),
                (
                    TagsData(pl.col('adjacent_tags_data'))
                    .filter(category=["genre", "mood", "tempo", "level", "topic"])
                    .sort_by_frequency()
                    .tags_with_frequencies()
                    .alias('adjacent_tag_frequency')
                ))\
            .rename({Track.country: 'country'})\
            .drop(Track.id, Track.release_date, Track.region,
                  Playlist.matched_terms_count)\
            .with_row_index(offset=1)\
            .collect(engine="streaming")

        st.dataframe(
            results_df,
            column_order=[
                'index',
                Track.name,
                Track.artists,
                TrackTags.tags,
                TrackTags.tag_frequency,
                TrackTags.playlist_counts_per_tag,
                'adjacent_tag_frequency',
                Track.url,
                Stats.playlist_count,
                Stats.dj_count,
                'hit_terms',
                Track.beats_per_minute,
                Playlist.matching_playlist_count,
                Track.has_queer_artist,
                Track.has_poc_artist,
                Playlist.name,
                PlaylistOwner.name,
                'country',
            ],
            column_config=track_columns | {
                'index': st.column_config.TextColumn(pinned=True),
                Track.name: st.column_config.TextColumn(pinned=True),
                TrackTags.tags: tag_manager.get_column_config(Tag.name),
                TrackTags.playlist_counts_per_tag: st.column_config.LineChartColumn(y_min=0)
            })

        if enable_show_containing_playlists and song_search_df.shape[0] <= 3:
            st.markdown("Playlists containing one or more of the songs above:")

            playlists_with_song = search_engine.data.playlists\
                .select(Playlist.id, Playlist.name)\
                .join(search_engine.data.playlist_tracks.select(Playlist.id, Track.id)
                      .join(song_search_df.lazy().select(Track.id), how='semi', on=Track.id),
                      how='semi', on=Playlist.id)

            min_playlist_size = 10
            prev_size = 2
            next_size = 2
            window_size = prev_size + 1 + next_size
            check_for_tags = ['mood:high energy']

            surrounding_tracks = search_engine.data.playlist_tracks\
                .join(playlists_with_song, how='semi', on=Playlist.id)\
                .select(Playlist.id, Track.id, PlaylistTrack.number().cast(pl.Int64))\
                .join(search_engine.data.tracks
                      .select(Track.id,
                              pl.concat_str(Track.name(), pl.lit(" - "), Track.artist_names()).alias('track_name'),
                              TrackTags.extract_tags().list.filter(pl.element().is_in(check_for_tags))),
                      how='inner', on=Track.id)\
                .join(song_search_df.lazy().select(Track.id, pl.lit(True).alias('is_target_track')),
                      how='left', on=Track.id)\
                .with_columns(TrackTags.tags().fill_null([]))\
                .with_columns(
                    pl.any_horizontal([
                        TrackTags.tags().list.contains(tag)
                        .or_(pl.col('is_target_track'))
                        for tag in check_for_tags
                    ]).alias('is_match'))\
                .sort(Playlist.id, PlaylistTrack.number)\
                .rolling(index_column=PlaylistTrack.number, period=f'{window_size}i', group_by=Playlist.id)\
                .agg(Track.id().get(prev_size),
                     pl.col('is_match').all(),
                     pl.col('track_name').alias('tracks'))\
                .filter(pl.col('is_match'))\
                .join(song_search_df.lazy().select(Track.id),
                      how='semi', on=Track.id)\
                .join(search_engine.data.playlists.select(Playlist.id, Playlist.name),
                      how='inner', on=Playlist.id)\
                .select(Playlist.name, PlaylistTrack.number, 'tracks', Playlist.url)

            st.dataframe(surrounding_tracks, column_config=playlist_columns)

        # playlists_text = ' '.join(song_search_df
        #                         .select(pl.col(Playlist.name).cast(pl.List(pl.String)))
        #                         .explode(Playlist.name)
        #                         .with_columns(pl.col(Playlist.name).str.to_lowercase().str.split(' '))
        #                         .explode(Playlist.name)
        #                         .unique()
        #                         .collect(engine='streaming')
        #                         [Playlist.name]
        #                         .to_list()
        #                         )

        # # Generate the WordCloud
        # if playlists_text:
        #         st.text('Playlist names also included')
        #         w = wordcloud.WordCloud(width=1800,
        #                         height=800,
        #                         background_color="white",
        #                         # stopwords=set(STOPWORDS),
        #                         min_font_size=10).generate(playlists_text)
        #         fig, ax = plt.subplots()
        #         ax.imshow(w)
        #         ax.axis('off')
        #         st.pyplot(fig)

        # creates a playlist based on the results
        # if st.button("Generate a playlist?", type="primary"):
        #         bpm_high = st.slider("BPM-high:", 85, 130, 101)
        #         bpm_med = st.slider("BPM-med:", 80, 100, 95)
        #         bpm_low = st.slider("BPM-low:", 85, 130, 88)
        #         how_many_songs = st.slider("Playlist length:", 3, 60, 18)

        st.text("Pretend you're Koichi with a ↗️↘️ playlist:")

        # no Koichis were harmed in the making of this shtity playlist, offended? possibly, but not harmed.
        pl_1 = (results_df
                .filter(pl.col(Track.beats_per_minute).gt(bpm_med) & pl.col(Track.beats_per_minute).le(bpm_high))
                .sort(Track.beats_per_minute, descending=True)
                .with_row_index('order', offset=1)
                # This gives them the order when combined with the other tracks
                .with_columns((pl.col('order') * 4) - 3,
                              level=pl.lit('high'))
                .head(100)
                # this shuffles that order so the songs aren't strictly high - low bpm
                # .with_columns(pl.col('order').shuffle())
                )

        pl_2 = (results_df
                .filter(pl.col(Track.beats_per_minute).gt(bpm_low) & pl.col(Track.beats_per_minute).le(bpm_med))
                .sort(Track.beats_per_minute, descending=True)
                .with_row_index('order', offset=1)
                .with_columns(pl.col('order') * 2,
                              level=pl.lit('medium'))
                .head(200)
                # .with_columns(pl.col('order').shuffle())
                )

        pl_3 = (results_df
                .filter(pl.col(Track.beats_per_minute).le(bpm_low) & pl.col(Track.beats_per_minute).gt(0))
                .sort(Track.beats_per_minute, descending=True)
                .with_row_index('order', offset=1)
                .with_columns((pl.col('order') * 4) - 1,
                              level=pl.lit('low'))
                .head(100)
                # .with_columns(pl.col('order').shuffle())
                )

        st.dataframe((pl.concat([pl_1, pl_2, pl_3])
                      .select('index', 'level', Track.beats_per_minute,
                              pl.all().exclude('index', Track.beats_per_minute, 'level'))
                      .sort('order')
                      .drop('order')
                      ),
                     column_config=link_columns)

        # # 1 2 3 2 1 2 3 2 1

        # attempt at better playlist generation
        # # Tag levels based on BPM
        # results_df2 = (results_df
        #                .with_columns(level = pl.when(pl.col(Track.beats_per_minute) > bpm_med)
        #                                         .then(pl.lit("high"))
        #                                         .when(pl.col(Track.beats_per_minute) > bpm_low)
        #                                         .then(pl.lit("medium"))
        #                                         .otherwise(pl.lit("low"))
        #                                )
        #                 )
        # # Get pools by level
        # high_df = results_df2.filter(pl.col("level") == "high")
        # medium_df = results_df2.filter(pl.col("level") == "medium")
        # low_df = results_df2.filter(pl.col("level") == "low")

        # # Build playlist
        # playlist_parts = []

        # for i in range(0,50):
        #         try:
        #                 # Step 1: High song (start)
        #                 h1 = high_df.sample(n=1, seed=42)
        #                 playlist_parts.append(h1)
        #                 prev_bpm = h1[Track.beats_per_minute][i]

        #                 # Step 2: Medium song
        #                 m1 = sample_with_bpm_range(medium_df, prev_bpm)
        #                 playlist_parts.append(m1)
        #                 prev_bpm = m1[Track.beats_per_minute][i]

        #                 # Step 3: Low song
        #                 l1 = sample_with_bpm_range(low_df, prev_bpm)
        #                 playlist_parts.append(l1)
        #                 prev_bpm = l1[Track.beats_per_minute][i]

        #                 # Step 4: Medium song
        #                 m2 = sample_with_bpm_range(medium_df, prev_bpm)
        #                 playlist_parts.append(m2)
        #                 prev_bpm = m2[Track.beats_per_minute][i]
        #         except:
        #                 pass

        # # Combine and add index
        # playlist_df = pl.concat(playlist_parts).with_row_index(name="order", offset=1)

        # # Display
        # st.dataframe((playlist_df
        #               .select('index', 'level', Track.beats_per_minute,
        #                       pl.all().exclude('index', Track.beats_per_minute, 'level'))
        #               .drop('order')
        #               ),
        # column_config=link_columns,
        # )

    st.markdown(f"#### ")


@immediate
@st.fragment
def section_find_playlist():
    # Courtesy of Vishal S.
    playlist_locator_toggle = st.toggle("Find a Playlist 💿")
    if not playlist_locator_toggle:
        return

    col1, col2 = st.columns(2)
    with col1:
        song_input = st.text_input("Contains the song:")
        artist_input = st.text_input("Contains the artist:")
    with col2:
        dj_input = st.text_input("DJ name:")
        not_dj_input = st.text_input("Exclude DJ name:")

    col1, col2 = st.columns(2)
    with col1:
        playlist_input = st.text_input("Playlist name:")
    with col2:
        not_playlist_input = st.text_input("Not in playlist name: ")

    col1, col2 = st.columns(2)
    with col1:
        tag_input = st.multiselect("Has tags:",
                                   options=tag_manager.get_tag_options(or_untagged=True),
                                   format_func=tag_manager.format_tag)
    with col2:
        not_tag_input = st.multiselect("Does not have tags:",
                                       options=tag_manager.get_tag_options(or_untagged=True),
                                       format_func=tag_manager.format_tag)

    col1, col2 = st.columns(2)
    with col1:
        is_social_input = st.checkbox(
            "Only social/party setlists",
            help="Only display playlists that have likely been played as-is at a party/social/other occasion.")

        has_date_input = st.checkbox(
            "Only dated playlists",
            help=("Only display playlists that have a date "
                  "(like `YYYY-MM-DD`, `dd.mm.`YYYY`, etc.) in their name"))

        not_has_date_input = st.checkbox(
            "Exclude dated playlists",
            help="Exclude playlists whose name contains a calendar date.")

        not_just_a_date_input = st.checkbox(
            "Not just a date",
            help="Exclude playlists whose name only consists of a date, and nothing else.")

        has_bpm_input = st.checkbox(
            "Only playlists with BPM",
            help=("Only display playlists that have a recognized BPM specification in their name."))

        not_has_bpm_input = st.checkbox(
            "Exclude playlists with BPM",
            help=("Exclude playlists that have a recognized BPM specification in their name."))
    with col2:
        min_song_count_input = st.number_input("Contains at least __ tracks", 0, None, 0)
        wcs_song_percent_input = st.slider("Contains __ % WCS songs", 0, 100, (0, 100),
                                           step=1, format="%u %%")
        tag_count_input = st.slider("Has __ tags", 0, 100, (0, 100))

    col1, col2 = st.columns(2)
    with col1:
        perform_search = st.button("Search playlists", type="primary")
    with col2:
        show_common_keywords = enable_show_playlist_keywords and st.toggle(
            "Show common keywords", help="Show common keywords in the names of the matched WCS playlists.")

    # if any(val for val in [playlist_input, song_input, dj_input]):
    if perform_search:
        log_query("Search playlists", {'song_input': song_input,
                                       'artist_input': artist_input,
                                       'dj_input': dj_input,
                                       'anti_dj_input': not_dj_input,
                                       'playlist_input': playlist_input,
                                       'anti_playlist_input': not_playlist_input,
                                       'is_social_input': is_social_input,
                                       'has_date_input': has_date_input,
                                       'min_song_count_input': min_song_count_input,
                                       })

        if tag_count_input[1] == 100:
            tag_count_input = (tag_count_input[0], None)

        # TODO: Expose additional query parameters in the UI
        playlist_search_df = search_engine.find_playlists(
            song_name=song_input,
            artist_name=artist_input,
            # country=...,
            dj_name=dj_input,
            dj_name_exclude=not_dj_input,
            playlist_include=playlist_input,
            playlist_exclude=not_playlist_input,
            playlist_is_social_set=is_social_input or None,
            playlist_has_date_in_title=False if not_has_date_input else True if has_date_input else None,
            min_song_count=min_song_count_input,
            playlist_stats_in_result=True,
            tag_include=tag_input,
            tag_exclude=not_tag_input,
            tracks_in_result=True,
            tracks_limit=30,
            sort_by=None,
            descending=True,
            limit=None,
        )

        if wcs_song_percent_input != (0, 100):
            playlist_search_df = playlist_search_df.filter(
                PlaylistStats.wcs_song_percent().is_between(
                    wcs_song_percent_input[0] / 100.0,
                    wcs_song_percent_input[1] / 100.0,
                    "both" if wcs_song_percent_input[1] == 100 else "left"))

        if not_just_a_date_input:
            playlist_search_df = playlist_search_df\
                .filter(~Playlist.name().is_in(Playlist.extracted_dates()))

        if has_bpm_input:
            playlist_search_df = playlist_search_df\
                .filter(contains_bpm_in_name(Playlist.name()))

        if not_has_bpm_input:
            playlist_search_df = playlist_search_df\
                .filter(~contains_bpm_in_name(Playlist.name()))

        if tag_count_input != (0, None):
            playlist_search_df = playlist_search_df\
                .filter(is_in_range(PlaylistTags.tags().list.len().fill_null(pl.lit(0, pl.UInt32)),
                                    tag_count_input))

        df = (playlist_search_df
              # .filter(~Playlist.is_social_set())
              .with_columns(Playlist.name, PlaylistTags.tags, Playlist.url, PlaylistOwner.name,
                            Playlist.matching_song_count, Stats.artist_count, Track.name)
              .sort(PlaylistStats.wcs_song_count, nulls_last=True, descending=True)
              .with_row_index(offset=1))

        if show_common_keywords:
            st.dataframe(df
                         .select(Playlist.name(),
                                 Playlist.id(),
                                 Playlist.name()
                                 .str.to_lowercase()
                                 .str.extract_all(r'\b\w+\b')
                                 .list.filter(pl.element().str.len_chars().gt(3))
                                 .alias('keyword'))
                         .explode('keyword')
                         .group_by('keyword')
                         .agg(Playlist.id().n_unique().alias(Stats.playlist_count), Playlist.name().head(30))
                         .filter(Stats.playlist_count().lt(10))
                         .sort(Stats.playlist_count, descending=True)
                         .collect(engine='streaming'))

        st.dataframe(df
                     .collect(engine='streaming'),
                     column_order=[
                         "index",
                         Playlist.name,
                         PlaylistTags.tags,
                         Playlist.url,
                         PlaylistOwner.name,
                         Playlist.matched_terms,
                         Playlist.matching_song_count,
                         Stats.song_count,
                         PlaylistStats.wcs_song_count,
                         PlaylistStats.wcs_song_percent,
                         Stats.artist_count,
                         Track.name,
                     ],
                     column_config=playlist_columns | {
                         PlaylistTags.tags: tag_manager.get_column_config(PlaylistTags.tags),
                     })

    st.markdown(f"#### ")


@immediate
@st.fragment
def section_tag_explorer():
    songs_by_tag_toggle = st.toggle("Explore Songs by Tags 🔍")
    if not songs_by_tag_toggle:
        return

    categories = [
        "genre",
        "mood",
        "timing",
        "structure",
        "topic",
        "language",
        "epoch",
        "seasonal",
        # "level", # "level:" tags aren't clean enough yet
    ]
    selected_tags = []

    for category in categories:
        category_metadata = tag_manager.config.metadata.get(category)
        category_title = category_metadata.get('selector_title')\
            or category_metadata.get('title')\
            or tag_manager.format_category(category)
        category_icon = tag_manager.config.icons_by_category.get(category)
        category_icon = f":{category_icon}:" if category_icon else None

        with st.expander(f":small[Select {category_title}(s)]", icon=category_icon):
            if category_metadata and (category_description := category_metadata.get('description')):
                st.markdown(f"{category_description}")

            if category_metadata and (category_comment := category_metadata.get('comment')):
                st.markdown(category_comment)

            selected_tags.extend(
                st.pills(f"**{category_title}**",
                         key=f"tag_explorer_category_{category}",
                         options=tag_manager.get_tag_options(category=category, for_selector=True),
                         format_func=partial(tag_manager.format_tag, as_short_name=True),
                         selection_mode="multi"))

    with st.container(horizontal=True, horizontal_alignment='left'):
        perform_search = st.button("Search songs", key="search_songs_by_tags", type="primary")

        create_playlist = (enable_spotify_integration
                           and spotify_client is not None
                           and st.button(":material/playlist_add: Create Spotify Playlist!",
                                         help="Create a Spotify playlist based on the search results."))

    if (perform_search or create_playlist) and not selected_tags:
        st.markdown(f":small[**No Search Results** \u2012 Select one or more tags before performing the search.]")
    elif (perform_search or create_playlist):
        tag_str = " ".join([f":gray-badge[{t}]" for t in selected_tags])

        playlist_creation_note = st.empty()
        st.markdown(f":small[**Songs tagged with**] {tag_str}")

        tagged_songs_df = search_engine\
            .find_songs_by_tags(tag_names_exact=selected_tags,
                                limit=200)\
            .with_row_index(offset=1)\
            .with_columns(
                TagsData(TrackTags.tags_data())
                .filter(category=["genre", "mood", "tempo", "level", "topic", "seasonal"])
                .sort_by_frequency()
                .tags_with_frequencies()
                .alias('all_tags'))\
            .collect(engine='streaming')

        if create_playlist and spotify_client is not None:
            human_readable_tag_names = " & ".join(map(tag_manager.format_tag, selected_tags))
            playlist_url = create_spotify_playlist(
                spotify_client,
                name = f"🪄 {human_readable_tag_names} tracks (from Westie Music Database)",
                description = "This playlist was generated using https://wcs-music-database.streamlit.app 🪄",
                tracks= tagged_songs_df[Track.url].to_list(),
            )

            with playlist_creation_note:
                st.markdown(f"Find your new playlist here: {playlist_url}")

        st.dataframe(tagged_songs_df,
                     column_order=[
                         'index',
                         Track.name,
                         Track.artists,
                         'matching_tags',
                         'all_tags',
                         Track.url,
                         'matching_tags_count',
                         'matching_tags_min_score',
                         'matching_tags_mean_score',
                         'matching_tags_sum',
                     ],
                     column_config=track_columns | {
                         'all_tags': st.column_config.Column(
                             help="**all\\_tags**:  \nAll of a song's tags."),
                         'matching_tags': st.column_config.Column(
                             help="**matching\\_tags**:  \nAll of a song's tags which match the query."),
                         'matching_tags_count': st.column_config.Column(
                             help="**matching\\_tags\\_count**:  \nThe number of matching tags."),
                         'matching_tags_min_score': st.column_config.Column(
                             help="**matching\\_tags\\_min\\_score**:  \nLowest confidence score within `matching_tags`."),
                         'matching_tags_mean_score': st.column_config.Column(
                             help="**matching\\_tags\\_mean\\_score**:  \nAverage confidence score within `matching_tags`."),
                         'matching_tags_sum': st.column_config.Column(
                             help="**matching\\_tags\\_min\\_score**:  \nSum of the playlist counts of all `matching_tags`."),
                     })

        # Show tags that are possibly related, and which we could possibly
        # recommend to the user. Determining the algorithm for choosing
        # the "best"/"most interesting" tags to recommend is currently
        # a work in progress.
        if enable_show_related_tags:
            interesting_tags = search_engine\
                .find_songs_by_tags(tag_names_exact=selected_tags)\
                .select(
                    pl.col('matching_tags_min_score'),
                    TagsData(TrackTags.tags_data())
                    .filter(category=categories)
                    .compute_confidence_scores()
                    .tags_data()
                    .list.filter(~TrackTag.tag.struct_field().is_in(selected_tags)))\
                .explode(TrackTags.tags_data())\
                .select(
                    pl.col('matching_tags_min_score'),
                    TrackTags.tags_data().struct.unnest())\
                .with_columns(
                    pl.min_horizontal(pl.col('matching_tags_min_score'), TrackTag.confidence()))\
                .filter(
                    TrackTag.tag().is_not_null())\
                .group_by(TrackTag.tag)\
                .agg(pl.len().alias(Stats.song_count),
                     TrackTag.confidence().mean().alias('confidence_average'),
                     TrackTag.confidence().sum().alias('confidence_sum'),
                     TrackTag.confidence().pow(2).sum().alias('confidence_pow2_sum'),
                     TrackTag.confidence().max().alias('confidence_max'),
                     pl.col('matching_tags_min_score').sum().alias('combined_confidence_sum'),
                     pl.col('matching_tags_min_score').max().alias('combined_confidence_max'))\
                .with_columns(
                    (pl.col('confidence_sum') * pl.col('confidence_max')).alias('confidence_score'))\
                .sort('confidence_sum', descending=True).with_row_index('confidence_sum_index', offset=1)\
                .sort('confidence_score', descending=True).with_row_index('confidence_score_index', offset=1)\
                .sort('combined_confidence_sum', descending=True).with_row_index('combined_confidence_sum_index', offset=1)\
                .sort('combined_confidence_max', descending=True).with_row_index('combined_confidence_max_index', offset=1)\
                .sort('confidence_average', 'confidence_sum')

            st.dataframe(interesting_tags, column_config=tag_columns)

    st.markdown(
        f"""
        Song tags are derived based on the playlists in which a song is contained.

        See the [keyword definition file]({keyword_file_url}) on GitHub to learn
        about the keywords that we currently recognize, and the tags they are assigned to.
        A lot of effort has went into making sure that playlists are tagged as accurately
        as possible, but there's only so much data that can be extracted from short playlist titles.

        As a general recommendation, searching for 1 - 3 tags works well.
        Using too many tags or very exotic tag combinations on the other hand will
        exclude so much of the dataset that what remains is bascially only noise.
        """)

    st.markdown("####")


@immediate
@st.fragment
def section_tag_insights():
    keyword_insights_toggle = st.toggle("Tag Insights 🏷️")
    if not keyword_insights_toggle:
        return

    st.markdown(f"\n\n\n#### Common Tags for Playlists:")
    st.markdown(
        f"""
        Disclaimer: Insights are based on a [manually defined list]({keyword_file_url}) of tags
        and aliases that is then used to extract keywords from playlist titles, and may not be
        accurate or representative of reality.
        """)

    tags_df = load_tags_data()
    tag_category_input = st.selectbox("Only show tags in category:",
                                      options=tag_manager.get_categories(or_all=True, preferred_first=True),
                                      format_func=tag_manager.format_category)

    show_wordcloud = st.toggle("Show wordcloud")

    if tag_category_input == TagManager.ALL_CATEGORIES:
        tag_category_input = ""

    filtered_tags_df = tags_df

    if tag_category_input:
        filtered_tags_df = filtered_tags_df\
            .filter(pl.col(Tag.category).eq(tag_category_input))

    if show_wordcloud:
        w = wordcloud.WordCloud(
            width=1800, height=800,
            background_color="white",
            # stopwords=set(STOPWORDS),
            min_font_size=10
        ).generate_from_frequencies({
            row[0]: float(row[1])
            for row in (filtered_tags_df
                        .filter(pl.col(Tag.short_name).is_not_null())
                        .select(Tag.short_name, Tag.playlist_count).iter_rows())
        })

        # As mentioned in the streamlit docs pyplot doesn't work well with threads,
        # so use a lock to protect it (as recommeded by the streamlit documentation)
        # See: https://docs.streamlit.io/develop/api-reference/charts/st.pyplot
        with _lock:
            fig, ax = plt.subplots()
            ax.imshow(w)
            ax.axis('off')
            st.pyplot(fig)

    st.dataframe(filtered_tags_df,
                 column_order=[
                     Tag.category, Tag.short_name, Tag.name,
                     Tag.playlist_count, Tag.max_playlist_count, Stats.song_count,
                     Playlist.name,
                 ], column_config=tag_columns | {
                     Tag.category: tag_manager.get_column_config(Tag.category),
                     Tag.short_name: tag_manager.get_column_config(Tag.short_name),
                     Tag.name: tag_manager.get_column_config(Tag.name),
                 })

    st.markdown(f"#### ")
    st.markdown(f"#### Tagged songs & playlists")

    col1, col2 = st.columns(2)
    with col1:
        show_all_tags = st.toggle("Show hidden tags",
                                  help=("Show all tags in the tag selection dropdown, "
                                        "even the ones that aren't normally useful for filtering "
                                        "(but are present in the data because they're used by our data pipeline)."))

    col1, col2 = st.columns(2)
    with col1:
        tag_input = st.selectbox("Show playlists & songs with tag:",
                                 options=tag_manager.get_tag_options(or_untagged=True, all_tags=show_all_tags),
                                 format_func=tag_manager.format_tag)
    with col2:
        anti_tag_input = st.selectbox("That are not also tagged with:",
                                      options=tag_manager.get_tag_options(or_empty=True, all_tags=show_all_tags),
                                      format_func=tag_manager.format_tag)

    if tag_input:
        st.markdown(f"Playlists tagged with _{tag_input}_:")

        tagged_playlists_df = search_engine\
            .find_playlists(tag_include=[tag_input],
                            tag_exclude=anti_tag_input)\
            .with_row_index(offset=1)

        if tag_input != TagManager.UNTAGGED:
            mean_confidences = search_engine.data.playlist_tracks\
                .join(tagged_playlists_df, how="semi", on=Playlist.id)\
                .join(search_engine.data.tracks.select(Track.id, TrackTags.tags_data),
                      how="left", on=Track.id)\
                .group_by(Playlist.id)\
                .agg(TagsData(TrackTags.tags_data())
                     .filter(tag=[tag_input])
                     .compute_confidence_scores()
                     .tags_data()
                     .list.agg(TrackTag.confidence.struct_field().first())
                     .mean()
                     .alias('mean_confidence'))

            tagged_playlists_df = tagged_playlists_df\
                .join(mean_confidences, how='left', on=Playlist.id)

        st.dataframe(tagged_playlists_df,
                     column_order=[
                         'index',
                         Playlist.name,
                         PlaylistOwner.name,
                         PlaylistTags.tags,
                         Playlist.country,
                         Playlist.region,
                         Playlist.extracted_dates,
                         Playlist.is_social_set,
                         PlaylistOwner.is_wcs_dj,
                         Stats.song_count,
                         Stats.artist_count,
                         Track.name,
                         Playlist.matching_song_count,
                         Playlist.matching_song_percent,
                         Playlist.matched_terms,
                         PlaylistStats.wcs_song_count,
                         PlaylistStats.total_song_count,
                         PlaylistStats.wcs_song_percent,
                     ],
                     column_config=playlist_columns < {
                         # "mean_confidence": st.column_config.ProgressColumn(min_value=0.0, max_value=1.0),
                     })

        st.markdown(f"Songs tagged with _{tag_input}_:")

        tagged_songs_df = search_engine\
            .find_songs_by_tag(tag_name_exact=tag_input,
                               not_tag_name_exact=anti_tag_input,
                               sort_by=TrackTag.matching_playlist_count,
                               descending=True)\
            .with_row_index(offset=1)\
            .collect(engine='streaming')

        st.dataframe(tagged_songs_df,
                     column_order=[
                         'index',
                         Track.name,
                         Track.artists,
                         TrackTag.tag,
                         TrackTag.matching_playlist_count,
                         TrackTag.Tag.playlist_percent,
                         TrackTag.Tag.playlist_count,
                         TrackTag.Track.playlist_percent,
                         TrackTag.Track.playlist_count,
                         Track.url,
                     ],
                     column_config={
                         **link_columns,
                         TrackTag.tag: tag_manager.get_column_config(TrackTag.tag),
                         TrackTag.matching_playlist_count: st.column_config.NumberColumn('#'),
                         TrackTag.Tag.playlist_count: st.column_config.NumberColumn('# tag'),
                         TrackTag.Tag.playlist_percent: st.column_config.ProgressColumn('% tag'),
                         TrackTag.Track.playlist_count: st.column_config.NumberColumn('# track'),
                         TrackTag.Track.playlist_percent: st.column_config.ProgressColumn('% track'),
                     })

        tagged_songs_df = tagged_songs_df\
            .limit(500)\
            .select(pl.all().name.map(lambda x: x.replace('.', '_')))

        show_graphs = st.toggle("Show graphs")

        if show_graphs:
            st.bar_chart(tagged_songs_df, x='index', y='matching_playlist_count', sort=False)
            st.bar_chart(tagged_songs_df, x='index', y='tag_playlist_percent', sort=False)
            st.bar_chart(tagged_songs_df, x='index', y='track_playlist_count', sort=False)
            st.bar_chart(tagged_songs_df, x='index', y='track_playlist_percent', sort=False)

        st.markdown(f"Playlists that contain many songs (but are not themselves) tagged with _{tag_input}_:")

        similar_playlists_df = search_engine\
            .find_playlists(
                song_tag_include=[tag_input],
                playlist_tag_exclude=[tag_input],
                tracks_in_result=True,
                tracks_limit=30,
                min_song_count=20,
                sort_by=[Playlist.matching_song_percent],
                descending=True)\
            .with_row_index(offset=1)

        st.dataframe(similar_playlists_df,
                     column_order=[
                         'index',
                         Playlist.name,
                         PlaylistOwner.name,
                         Playlist.matching_song_count,
                         Playlist.matching_song_percent,
                         PlaylistTags.tags,
                         Playlist.country,
                         Playlist.region,
                         Playlist.extracted_dates,
                         Playlist.is_social_set,
                         PlaylistOwner.is_wcs_dj,
                         Stats.song_count,
                         Stats.artist_count,
                         Track.name,
                         PlaylistStats.wcs_song_count,
                         PlaylistStats.total_song_count,
                         PlaylistStats.wcs_song_percent,
                     ],
                     column_config=playlist_columns | {
                         # "mean_confidence": st.column_config.ProgressColumn(min_value=0.0, max_value=1.0),
                     })


@st.cache_data
def djs_data():
    return load_search_engine().get_dj_stats(playlist_limit=30, dj_limit=2000).collect(engine='streaming')


@immediate
@st.fragment
def section_dj_insights():
    # Courtesy of Lino V.
    search_dj_toggle = st.toggle("DJ insights 🎧")
    if not search_dj_toggle:
        return

    dj_col1, dj_col2 = st.columns(2)
    with dj_col1:
        dj_input = st.text_input("DJ name/ID (ex. Kasia Stepek or 1185428002)")
    with dj_col2:
        dj_playlist_input = st.text_input("DJ playlist name:")

    if not dj_input and not dj_playlist_input:
        djs_data_df = djs_data()
        st.dataframe(djs_data_df, column_config=dj_columns)

    # else:
    if st.button("Search djs", type="primary"):
        log_query("Search djs", {'dj_input': dj_input,
                                 'dj_playlist_input': dj_playlist_input,
                                 })

        dj_search_df = search_engine.find_djs(
            dj_name=dj_input,
            playlist_name=dj_playlist_input,
            dj_limit=100,
            playlist_limit=30,
        ).collect(engine='streaming')

        st.dataframe(dj_search_df, column_config=dj_columns)

        total_djs_from_search = dj_search_df\
            .select(pl.n_unique(PlaylistOwner.name))[PlaylistOwner.name][0]

        if total_djs_from_search > 0 and total_djs_from_search <= 10:  # so it doesn't have to process if nothing

            djs_music = (search_engine.find_songs(dj_name=dj_input)
                         .select(Track.id, Track.name, Track.artist_names, PlaylistOwner.name,
                                 Stats.dj_count, Stats.playlist_count, Playlist.name, Track.url))

            st.markdown(f"Music unique to _{', '.join(dj_input.split(','))}_")
            st.dataframe(djs_music.filter(pl.col(Stats.dj_count).eq(1))
                         .group_by(pl.all().exclude(Playlist.name))
                         .agg(Playlist.name)
                         .sort(Stats.playlist_count, descending=True)
                         .drop(Track.id)
                         .head(100),
                         column_config=track_columns)

            # st.markdown(f"Popular music _{', '.join(dj_input)}_ doesn't play")
            # st.dataframe(others_music.join(djs_music, how='anti',
            #                 on=[Track.name, Stats.dj_count,
            #                 Stats.playlist_count, Track.url])
            #         .group_by(pl.all().exclude(PlaylistOwner.name))
            #         .agg(PlaylistOwner.name)
            #         .with_columns(pl.col(PlaylistOwner.name).list.head(30))
            #         .sort(Stats.dj_count, Stats.playlist_count, descending=True)
            #         .head(200)
            #         .collect(engine='streaming'),
            #         column_config=link_columns)

    st.markdown(f"#### Compare DJs:")
    # dj_list = sorted(df
    #                  .select(PlaylistOwner.name)
    #                  .cast(pl.String)
    #                  .unique()
    #                  .drop_nulls()
    #                  .collect(engine='streaming')
    #                  [PlaylistOwner.name]
    #                  .to_list()
    #                  )

    # st.dataframe(df
    #                 .group_by(PlaylistOwner.name)
    #                 .agg(song_count = pl.n_unique(Track.name),
    #                         playlist_count = pl.n_unique(Playlist.name),
    #                         dj_count = pl.n_unique(PlaylistOwner.name),
    #                         )
    #                 .sort(PlaylistOwner.name)
    #                 .collect(engine='streaming')
    #         )

    # djs_selectbox = st.multiselect("Compare these DJ's music:", dj_list)
    compare_1, compare_2 = st.columns(2)
    with compare_1:
        dj_compare_1 = st.text_input("DJ/user 1 to compare:")
    with compare_2:
        dj_compare_2 = st.text_input("DJ/user 2 to compare:")

    if st.button("Compare DJs/users", type="primary"):
        log_query("Search djs", {'dj_compare_1': dj_compare_1,
                                 'dj_compare_2': dj_compare_2,
                                 })

        st.dataframe(search_engine.find_songs(dj_name=f'{dj_compare_1},{dj_compare_2}')
                     .group_by(PlaylistOwner.name)
                     # .with_columns(pl.concat_list(Track.name, Track.artist_names).alias('track.full_name'))
                     .agg(pl.n_unique(Track.id).alias(Stats.song_count),
                          pl.n_unique(Playlist.name).alias(Stats.playlist_count))
                     .sort(PlaylistOwner.name)
                     .collect(engine='streaming'))

        dj_1_df = search_engine.find_songs(dj_name=dj_compare_1).select(
            Track.name, Track.url, Stats.dj_count, Stats.playlist_count)
        dj_2_df = search_engine.find_songs(dj_name=dj_compare_2).select(
            Track.name, Track.url, Stats.dj_count, Stats.playlist_count)

        st.markdown(f"Music _{dj_compare_1}_ has, but _{dj_compare_2}_ doesn't.")
        st.dataframe(dj_1_df
                     .join(dj_2_df, how='anti', on=[Track.name, Track.url])
                     .unique()
                     .sort(Stats.dj_count, descending=True)
                     .head(500),
                     column_config=track_columns)

    st.markdown(f"#### ")


@st.cache_data
def region_data():
    return (search_engine.get_region_stats()
            .collect(engine='streaming'))


@st.cache_data
def country_data():
    return (search_engine.get_country_stats()
            .collect(engine='streaming'))


@immediate
@st.fragment
def section_geographic_insights():
    # Courtesy of Lino V.
    geo_region_toggle = st.toggle("Geographic Insights 🌎")
    if not geo_region_toggle:
        return

    st.markdown(f"\n\n\n#### Region-Specific Music:")
    st.text(f"Disclaimer: Insights are based on available data and educated guesses - which may not be accurate or representative of reality.")

    st.dataframe(region_data())
    st.dataframe(country_data())
    regions = ['Select One', 'Europe',
               'North America', 'MENA', 'Oceania', 'Asia']
    region_selectbox = st.selectbox("Which Geographic Region would you like to see?",
                                    regions)

    if region_selectbox != 'Select One':
        st.markdown(f"#### What are the most popular songs only played in {region_selectbox}?")

        region_df = (pl.scan_parquet('processed_data/data_unique_per_region.parquet')
                     #  .pipe(wcs_specific)
                     .filter(pl.col('region').cast(pl.String) == region_selectbox,
                             # pl.col('geographic_region_count').eq(1)
                             )
                     # .group_by(Track.name, Track.url, Stats.dj_count, Stats.playlist_count, 'region', 'geographic_region_count')
                     # .agg(pl.col(PlaylistOwner.name).unique())
                     # .with_columns(pl.col(PlaylistOwner.name).list.unique())
                     # .unique()
                     .rename({'song_url': Track.url, 'owner.display_name': PlaylistOwner.name})
                     .sort(Stats.playlist_count, Stats.dj_count, descending=True))

        st.dataframe(region_df.head(1000).collect(engine='streaming'), column_config=track_columns)

    st.markdown(f"#### Comparing Countries' music:")
    countries_selectbox = st.multiselect(
        "Compare these countries' music:",
        countries,
        max_selections=2,
    )

    if st.button("Compare countries", type="primary"):
        log_query("Comparing Countries' music", {
                  'countries_selectbox': countries_selectbox})

        countries_df = search_engine.find_songs(country=countries_selectbox).filter(
            pl.col(Stats.dj_count).gt(3),
            pl.col(Stats.playlist_count).gt(3)
        )

        country_1_df = (countries_df
                        .filter(pl.col(Track.country).list.contains(countries_selectbox[0]))
                        .select(pl.col(Track.country).alias('country'), Track.id,
                                Track.name, Track.url, Stats.dj_count, Stats.playlist_count))

        country_2_df = (countries_df
                        .filter(pl.col(Track.country).list.contains(countries_selectbox[1]))
                        .select(pl.col(Track.country).alias('country'), Track.id,
                                Track.name, Track.url, Stats.dj_count, Stats.playlist_count))

        st.text(f"{countries_selectbox[0]} music not in {countries_selectbox[1]}")
        compare_df = (country_1_df.join(country_2_df, how='anti', on=Track.id)
                      .unique()
                      .drop(Track.id)
                      .sort(Stats.dj_count, descending=True)
                      .head(300))
        print(compare_df.explain(engine='streaming', format='plain'))
        st.dataframe(compare_df.collect(engine='streaming'), column_config=track_columns)
        st.markdown(f"#### ")


@st.cache_data
def top_related_songs():
    return (search_engine.find_related_songs('any', return_pairs=True, limit=1000)[1]
            .select(TrackAdjacent.FirstTrack.name, TrackAdjacent.FirstTrack.artists,
                    TrackAdjacent.times_played_together,
                    TrackAdjacent.SecondTrack.name, TrackAdjacent.SecondTrack.artists)
            .collect(engine='streaming'))


@immediate
@st.fragment
def section_songs_most_played_together():
    # Courtesy of Vincent M.
    songs_together_toggle = st.toggle("Songs most played together")
    if not songs_together_toggle:
        return

    song_combo_col1, song_combo_col2 = st.columns(2)
    with song_combo_col1:
        song_input = st.text_input("Song Name:")
    with song_combo_col2:
        artist_name_input = st.text_input("Song artist name:")

    if not song_input and not artist_name_input:
        st.markdown("#### Most common songs to play next to each other")
        top_related_songs_df = top_related_songs()
        st.dataframe(top_related_songs_df, column_config=track_columns)

    if st.button("Search songs played together", type="primary"):
        st.markdown("#### Songs"
                    + (f" matching _{song_input}_" if song_input else "")
                    + (f" by _{artist_name_input}_" if artist_name_input else "")
                    + ":")
        st.dataframe(search_engine.find_songs(song_name=song_input, artist_name=artist_name_input, limit=100),
                     column_order=[Track.name, Track.artists, Track.url,
                                   Track.beats_per_minute, Track.release_date],
                     column_config=track_columns)

        st.markdown(f"#### Most common songs to play after _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('next', song_name=song_input, artist_name=artist_name_input)[1],
                     column_order=[Track.name, Track.artists, TrackAdjacent.times_played_together,
                                   Track.url, Track.beats_per_minute, Track.release_date],
                     column_config=track_columns)

        st.markdown(f"#### Most common songs to play before _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('prev', song_name=song_input, artist_name=artist_name_input)[1],
                     column_order=[Track.name, Track.artists, TrackAdjacent.times_played_together,
                                   Track.url, Track.beats_per_minute, Track.release_date],
                     column_config=track_columns)

        st.markdown(f"#### Most common songs to play before _or_ after _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('any', song_name=song_input, artist_name=artist_name_input)[1],
                     column_order=[Track.name, Track.artists, TrackAdjacent.times_played_together,
                                   Track.url, Track.beats_per_minute, Track.release_date],
                     column_config=track_columns)

    st.link_button("Andreas' connected-songs visualization!",
                   'https://loewclan.de/song-galaxy/')
    st.markdown(f"#### ")


class SongData:
    @property
    def data(self):
        raise NotImplementedError()

    @property
    def id(self) -> str:
        return self.data[Track.id]

    @property
    def track_name(self) -> str:
        return self.data[Track.name]

    @property
    def artist_names(self) -> str:
        return self.data[Track.artist_names]

    @property
    def track_title(self) -> str:
        return f"{self.track_name} \u2013 {self.artist_names}"

    @property
    def playlist_count(self) -> int:
        return self.data[Stats.playlist_count]


@dataclass
class SongSearcher(SongData):
    song_name: str
    artist_name: str

    def __post_init__(self):
        self.song_df = search_engine\
            .find_songs(song_name=self.song_name,
                        artist_name=self.artist_name,
                        sort_by=Stats.playlist_count,
                        limit=1)\
            .collect(engine='streaming')

    @property
    def found(self):
        return len(self.song_df) == 1

    @property
    def data(self):
        return self.song_df.to_dicts()[0]

    def find_playlist_tracks(self):
        return search_engine.data.playlist_tracks\
            .filter(Track.id().eq(self.id))\
            .select(Playlist.id, Track.id, PlaylistTrack.number)

    def find_playlist_tracks_in(self, playlists: pl.LazyFrame):
        return search_engine.data.playlist_tracks\
            .join(playlists, how='semi', on=Playlist.id)\
            .filter(Track.id().eq(self.id))\
            .select(Playlist.id, Track.id, PlaylistTrack.number)\
            .sort(Playlist.id, PlaylistTrack.number)

    def find_indices(self, name: str, in_playlists: pl.LazyFrame):
        return self.find_playlist_tracks_in(in_playlists)\
            .group_by(Playlist.id)\
            .agg(PlaylistTrack.number().sort().alias(f'{name}.indices'))

    def find_playlists(self):
        return self.find_playlist_tracks()\
            .select(Playlist.id().unique().sort())

    @staticmethod
    def find_playlists_with(all_of: list["SongSearcher"]) -> pl.LazyFrame | None:
        """Get all playlists that contain all of the specified songs."""
        playlists = None
        for song in all_of:
            playlists = (song.find_playlists() if playlists is None
                         else playlists.join(song.find_playlists(), how='semi', on=Playlist.id))
        return playlists


@dataclass
class SongComparison:
    song1: SongSearcher
    song2: SongSearcher

    def total_playlist_count(self) -> int:
        """Count all playlists that contain one or both songs."""
        return self.song1.playlist_count + self.song2.playlist_count - self.shared_playlist_count()

    def shared_playlist_count(self) -> int:
        """Count all playlists that contain both songs."""
        return SongSearcher.find_playlists_with(all_of=[self.song1, self.song2])\
            .select(Playlist.id().n_unique().alias(Stats.playlist_count))\
            .collect(engine='streaming')[Stats.playlist_count][0]

    def find_shared_playlists(
        self,
        *,
        playlist_in_result: bool = False,
        indices_in_result: bool = False,
        min_distance_in_result: bool = False
    ) -> pl.LazyFrame:
        """Get playlists that contain both songs."""
        playlists = SongSearcher.find_playlists_with(all_of=[self.song1, self.song2])

        if playlist_in_result:
            playlists = playlists\
                .join(search_engine.data.playlists, how='inner', on=Playlist.id)

        if indices_in_result:
            song1_indices = self.song1.find_indices('song1', in_playlists=playlists)
            song2_indices = self.song2.find_indices('song2', in_playlists=playlists)
            indices = song1_indices.join(song2_indices, how='inner', on=Playlist.id)

            playlists = playlists\
                .join(indices, how='inner', on=Playlist.id)

        if min_distance_in_result:
            song1_playlist_tracks = self.song1.find_playlist_tracks_in(playlists)
            song2_playlist_tracks = self.song2.find_playlist_tracks_in(playlists)
            min_distances = song1_playlist_tracks\
                .join_asof(song2_playlist_tracks, strategy='nearest',
                           coalesce=False, check_sortedness=False,
                           on=PlaylistTrack.number, by=Playlist.id)\
                .group_by(Playlist.id)\
                .agg((PlaylistTrack.number().cast(pl.Int16) - pl.col(f"{PlaylistTrack.number}_right").cast(pl.Int16)).abs().min().alias('min_distance'))

            playlists = playlists\
                .join(min_distances, how='inner', on=Playlist.id)

        return playlists


# Courtesy of Lukas W.
song_distance_toggle = enable_song_distance and st.toggle("Song distance")

if song_distance_toggle:
    st.markdown("What is the average distance between two songs in our playlists?")

    song_combo_col1, song_combo_col2 = st.columns(2)
    with song_combo_col1:
        song1_name_input = st.text_input("Song 1 name:", "Josephine")
        song1_artist_input = st.text_input("Song 1 artist name:", "RITUAL")
    with song_combo_col2:
        song2_name_input = st.text_input("Song 2 name:", "Don't")
        song2_artist_input = st.text_input("Song 2 artist name:", "Ed Sheeran")

    st.markdown("Comparing these two songs:")

    song1 = SongSearcher(song_name=song1_name_input,
                         artist_name=song1_artist_input)

    song2 = SongSearcher(song_name=song2_name_input,
                         artist_name=song2_artist_input)

    song_1_and_2_df =\
        pl.concat([song1.song_df, song2.song_df])\
          .select(Track.name,
                  Track.artists,
                  Track.url,
                  Stats.playlist_count,
                  Stats.dj_count)

    st.dataframe(song_1_and_2_df, column_config=track_columns)

    if song1.found and song2.found:
        song_comparison = SongComparison(song1, song2)
        common_playlists_df = song_comparison.find_shared_playlists()

        st.markdown("How many playlists contain both songs?")

        common_playlists_count = song_comparison.shared_playlist_count()

        chart_data = pl.DataFrame({
            'index': [
                1,
                2,
                3,
            ],
            'track_id': [
                song1.id,
                song2.id,
                f"{song1.id}+{song2.id}",
            ],
            'track_name': [
                song1.track_name,
                song2.track_name,
                " / ".join([song1.track_name, song2.track_name]),
            ],
            'track_artists': [
                song1.artist_names,
                song2.artist_names,
                " / ".join([song1.artist_names, song2.artist_names]),
            ],
            'track_title': [
                song1.track_title,
                song2.track_title,
                'Both',
            ],
            'playlist_count': [
                song1.playlist_count - common_playlists_count,
                song2.playlist_count - common_playlists_count,
                common_playlists_count,
            ],
        })

        chart = alt.Chart(chart_data).transform_joinaggregate(
            total_playlist_count='sum(playlist_count)',
        ).transform_calculate(
            playlist_percent="datum.playlist_count / datum.total_playlist_count"
        ).mark_arc().encode(
            theta=alt.Theta('playlist_count').title("# of Playlists"),
            color=alt.Color('track_title:N').title("Track Title"),
            tooltip=[
                alt.Tooltip('track_name:N').title("Track Name"),
                alt.Tooltip('track_artists:N').title("Track Artists"),
                alt.Tooltip('playlist_count:Q').title("# of Playlists"),
                alt.Tooltip('playlist_percent:Q', format='.0%').title("Relative Percentage"),
            ],
        )

        st.altair_chart(chart)

        st.markdown("What is the average distance between these two tracks in playlists that contain both?")

        st.dataframe(song_comparison.find_shared_playlists(playlist_in_result=True,
                                                           indices_in_result=True,
                                                           min_distance_in_result=True),
                     column_order=[Playlist.name, 'min_distance', 'song1.indices', 'song2.indices'])

        chart_data = song_comparison\
            .find_shared_playlists(playlist_in_result=True,
                                   indices_in_result=True,
                                   min_distance_in_result=True)\
            .select(Playlist.id,
                    Playlist.name().alias('playlist_name'),
                    Stats.song_count().alias('playlist_size'),
                    'min_distance')\
            .collect(engine='streaming')

        chart = alt.Chart(chart_data).mark_point().encode(
            y='min_distance:Q',
            x='playlist_size:Q',
            tooltip=['playlist_name', 'playlist_size', 'min_distance']
        ).interactive()

        st.altair_chart(chart)


@st.cache_data(persist=True)
def get_song_comparison_data(song_one: SongSearcher, song_two: SongSearcher) -> pl.DataFrame:
    return SongComparison(song_one, song_two)\
        .find_shared_playlists(playlist_in_result=True,
                               min_distance_in_result=True)\
        .select(Playlist.id().alias('playlist_id'),
                Playlist.name().alias('playlist_name'),
                Stats.song_count().alias('playlist_size'),
                'min_distance')\
        .with_columns(pl.lit(" / ".join([song_one.track_title, song_two.track_title])).alias('track_title'),
                      pl.lit(" / ".join([song_one.track_name, song_two.track_name])).alias('track_name'),
                      pl.lit(song_one.track_name).alias("song1_track_name"),
                      pl.lit(song_two.track_name).alias("song2_track_name"),
                      pl.lit(song_one.track_title).alias("song1_track_title"),
                      pl.lit(song_two.track_title).alias("song2_track_title"))\
        .collect(engine='streaming')


@immediate
@st.fragment
def section_song_distance():
    if not enable_song_distance:
        return

    if not st.toggle("Compare multiple songs"):
        return

    def show_chart(chart_data):
        track_selection = alt.selection_point(fields=['track_title'])
        playlist_selection = alt.selection_point(fields=['playlist_id'])

        color = (
            alt.when(playlist_selection & track_selection)
            .then(alt.Color('track_name:N').scale(scheme='category10').legend(None))
            .otherwise(alt.value("lightgray"))
        )
        zindex = (
            alt.when(track_selection)
            .then(alt.value(1))
            .otherwise(alt.value(0))
        )

        legend = alt.Chart(chart_data).mark_point().encode(
            alt.Y('track_name:N').axis(orient='right'),
            color=color,
        ).add_params(
            track_selection,
        )

        chart = alt.Chart(chart_data).mark_point().encode(
            y=alt.Y('min_distance:Q').scale(domainMin=0),
            x=alt.X('playlist_size:Q').scale(domainMin=0),
            color=color,
            order=zindex,
            tooltip=['song1_track_title', 'song2_track_title', 'playlist_name', 'playlist_size', 'min_distance'],
        ).transform_calculate(
            min_distance_divided_by_playlist_size=alt.datum.min_distance / alt.datum.playlist_size
        ).transform_filter(
            alt.datum.playlist_size < 240
        ).add_params(
            playlist_selection,
        ).interactive()

        st.altair_chart(chart | legend)

    @st.cache_data(persist=True)
    def get_chart_data():
        song1 = SongSearcher(song_name="Josephine", artist_name="RITUAL")
        song2 = SongSearcher(song_name="Redbone (Stay Woke)", artist_name="Nath Brooks")
        song3 = SongSearcher(song_name="Don't", artist_name="Ed Sheeran")
        song4 = SongSearcher(song_name="Galway Girl", artist_name="Ed Sheeran")

        return pl.concat([
            get_song_comparison_data(song1, song2),
            get_song_comparison_data(song3, song4),
            get_song_comparison_data(song1, song3),
            get_song_comparison_data(song2, song3),
            get_song_comparison_data(song1, song4),
            get_song_comparison_data(song2, song4),
        ])

    show_chart(get_chart_data())


@st.cache_data
def songs_by_year():
    current_year: Final = time.localtime().tm_year
    return search_engine.get_popularity_over_time(interval='year', year_range=(2000, current_year))\
        .collect(engine='streaming')


@immediate
@st.fragment
def section_song_popularity():
    song_popularity_toggle = st.toggle("Song popularity over time 📊")
    if not song_popularity_toggle:
        return

    DAY = 'day'
    INTERVALS: Final = {
        'year': 'Yearly',
        'month': 'Monthly',
        'quarter': 'Quarterly',
        'week': 'Weekly',
        'day': 'Daily',
    }
    RELATIVE_POPULARITY: Final = 'relative_popularity'
    PLAYLIST_TRACK_COUNT: Final = 'playlist_track_count'

    song_combo_col1, song_combo_col2 = st.columns(2)
    with song_combo_col1:
        song_input = st.text_input("Song Name/ID:")
        only_socials_input = st.checkbox("Only socials")
    with song_combo_col2:
        artist_name_input = st.text_input("Song artist name:")
        interval_input = st.selectbox(label="Interval:", options=INTERVALS.keys(),
                                      format_func=lambda opt: INTERVALS.get(opt, opt))
        min_plays_input = st.number_input("Only compare against tracks with at least __ plays in a given interval:",
                                          value=0, min_value=0, max_value=100, step=1)

    search_button = st.button("Show song popularity over time", type="primary")

    popularity_df: pl.DataFrame | None = None
    is_search_result: bool = False

    if not song_input and not artist_name_input and not search_button:
        popularity_df = songs_by_year()
        interval_input = 'year'

    if search_button:
        is_search_result = True

        # We're not sure why, but our dataset contains quite a few
        # playlist entries with an added_at date that is a few years
        # in the future... just filter these out for now.
        current_year: Final = time.localtime().tm_year
        popularity_df = search_engine.get_popularity_over_time(
            song_name=song_input,
            artist_name=artist_name_input,
            playlist_is_social_set=only_socials_input,
            interval=interval_input,
            min_plays=min_plays_input,
            year_range=(2000, current_year))\
            .collect(engine='streaming')

    if popularity_df is not None:
        popularity_max = popularity_df.lazy()\
            .select(pl.col(PLAYLIST_TRACK_COUNT).max(),
                    pl.col(RELATIVE_POPULARITY).max(),
                    pl.col(Stats.song_count).max())\
            .collect(engine='streaming')

        st.markdown(f"#### Playlist track entries by {interval_input}")

        if is_search_result:
            if interval_input != "year":
                st.markdown("Be aware that song popularity statistics on intervals shorter than"
                            "a year are heavily skewed by which events are contained in our dataset, "
                            "and should therefore be taken with a grain of salt.")

            st.markdown("Relative popularity is calculated based on the number of plays "
                        "the average song has received in the given interval.")
            st.bar_chart(popularity_df, x=interval_input, y=RELATIVE_POPULARITY)

        st.dataframe(popularity_df,
                     column_config={
                         DAY: st.column_config.DateColumn(),
                         PLAYLIST_TRACK_COUNT: st.column_config.ProgressColumn(
                             min_value=0, format='localized',
                             max_value=popularity_max[PLAYLIST_TRACK_COUNT].first()),
                         Stats.song_count: st.column_config.ProgressColumn(
                             min_value=0, format='localized',
                             max_value=popularity_max[Stats.song_count].first()),
                         RELATIVE_POPULARITY: st.column_config.ProgressColumn(
                             min_value=0, format='percent',
                             max_value=popularity_max[RELATIVE_POPULARITY].first())})


@immediate
@st.fragment
def section_find_lyrics():
    lyrics_toggle = st.toggle("Search lyrics 📋")
    if not lyrics_toggle:
        return

    st.write(f"from {lyrics_count:,} songs")
    lyrics_col1, lyrics_col2 = st.columns(2)
    with lyrics_col1:
        song_input = st.text_input("Song:")
        lyrics_input = st.text_input("In lyrics:")

    with lyrics_col2:
        artist_input = st.text_input("Artist:")
        anti_lyrics_input = st.text_input("Not in lyrics:")

    if st.button("Search lyrics", type="primary"):
        st.dataframe(
            search_engine.find_songs(
                song_name=song_input,
                artist_name=artist_input,
                playlist_in_result=False,
                playlist_track_in_result=False,
                lyrics_include=lyrics_input,
                lyrics_exclude=anti_lyrics_input,
                lyrics_in_result=True,
            )
            # TODO: See whether we can remove this because we have implemented deduplication
            # Otherwise there will be multiple rows for each song variation
            .group_by(Track.name, Track.artist_names)
            .agg(pl.col(Track.url).first(),
                 pl.col(Track.artists).first(),
                 pl.col(TrackLyrics.matched_lyrics).first(),
                 # TODO: Adding up playlist_count may lead to slightly inflated numbers
                 #       when different instances of a song are include in a single playlist.
                 pl.col(Stats.playlist_count).sum(),
                 # TODO: The merged dj_count will very likely be too large, since
                 #       it double-counts DJs if multiple instances of a song are present.
                 #       The only good way to deal with this is to unify those instances
                 #       during the pre-processing of the data.
                 pl.col(Stats.dj_count).sum())
            .sort(pl.col(TrackLyrics.matched_lyrics).list.len(), descending=True, nulls_last=True),
            column_config=track_columns)


st.markdown("# ")
st.markdown("# ")
st.markdown("#### WCS resources/apps by others:")  # Thank you, Clara!
st.link_button('Routine Database 😯',
               url='https://wcs-routine-database.streamlit.app/')
# st.link_button('Follow me so I can add you to the database!',
#                'https://open.spotify.com/user/225x7krl3utkpzg34gw3lhycy')
st.link_button('📍 Find a WCS class near you!',
               url='https://www.affinityswing.com/classes')
st.link_button('📆 Westie App Events Calendar',
               'https://westie-app.dance/calendar')
st.link_button('💃⏱️ Dance Metronome',
               url='https://loewclan.de/metronome/')
st.link_button('Weekenders Events Calendar',
               'https://weekenders.dance/')
st.link_button('Leave feedback/suggestions!/Report issues/bugs',
               url='https://forms.gle/19mALUpmM9Z5XCA28')


st.markdown("""####
### Westie Music Database FAQ
#### How can I help?
* Make lots of playlists with descriptive names! The more the better!
* Add "WCS" to your playlist name
* Add "yyyy-mm-dd" date (or variation) when you played the DJ set for a social
* Let me know the country of a user - helps our geographic insights!
* DJs: Send me your VirtualDJ backup database file (it only includes the metadata, not the actual song files)

#### What can the Westie Music Database tell me?
* What music was played at Budafest, but NOT at Westie Spring Thing (Courtesy of Nicole Y!)
* Top 1000 songs
* Event sets
* Most popular songs and playlists for:
        * Late-night
        * Competitions
        * Beginners
        * BPM-specific
        * Era's (80's, 90's, etc)
        * Holidays
        * Particular country (Germany)
* Comparing 2 DJ's music
* Songs unique to a particular DJ
* Comparing a country's music
* Songs unique to a country
* Top Songs per geographic region/country
* Songs only played in a country
* Finding songs by lyrics
* Most popular songs played together

#### Where does the data come from?
- I find westies on Spotify, and use Spotify's API to grab all their public playlists.
- Currently trying to incorporate DJ data from VirtualDJ

#### Doesn't that mean there's some non-WCS music?
* Correct, not all music is WCS specific, but I filter out the bulk of it (Tango/Salsa/Etc.), and the music that's left rises to the top due to the amount of westies adding it to their playlists. Eg. If we all listen to non-westible show tunes, those songs might rise to the top, but we also have the # of playlists to sort by - Chunks, might appear in multiple playlists per spotify profile, but Defying Gravity would be in fewer.

#### I'm not a DJ and don't have a lot of playlists, can I be included?/why am I included?
* Please click the feedback form link and add your profile link and location so I can include you!
* The wonderful thing about aggregation on this scale is that even your 1 or 2 wcs playlists will still help!
* Some people have many playlists, well labeled, and others have a single "WCS" playlist with 1400 songs! All are helpful in their own way!

#### Artists are kinda messed up
* Yes, they're a pain, I'll handle it eventually, right now I'm ignoring it.

#### Errors:
* Please report any errors you notice, or anything that doesn't make sense and I'll try to get to it!

#### Things to consider:
* Since the majority of data is based on user adding songs to their own playlists, user-generated vs DJ-generated, the playlists may not reflect actual played sets (except when specified). The benefit, while I work on rounding up DJs not on Spotify, is that we get to see the ground truth of what users actually enjoy (such as songs missed by the GSDJ Top 10 lists).
""")
