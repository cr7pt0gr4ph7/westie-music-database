from threading import RLock
from typing import Final

import streamlit as st
import wordcloud
import math
import matplotlib.pyplot as plt
import polars as pl
import polars.selectors as cs
import psutil
import time

from utils.common.columns import pull_columns_to_front
from utils.common.logging import log_query
from utils.keyword_data import load_keyword_colors
from utils.playlist_classifiers import extract_date_types_from_name, extract_date_strings_from_name
from utils.pull_data import automatically_pull_data_if_needed
from utils.search import SearchEngine, TRACK_TAGS_DATA_FILE
from utils.tables import Playlist, PlaylistOwner, PlaylistTrack, Stats, Tag, Track, TrackAdjacent, TrackLyrics, TrackTag

# As mentioned in the streamlit docs pyplot doesn't work well with threads,
# so use a lock to protect it (as recommeded by the streamlit documentation)
# See: https://docs.streamlit.io/develop/api-reference/charts/st.pyplot
_lock = RLock()

# avail_threads = pl.threadpool_size()
pl.Config.set_tbl_rows(100).set_fmt_str_lengths(100)
pl.enable_string_cache()  # for Categoricals
# st.text(f"{avail_threads}")

# Only check once per session
if "pull_data" not in st.session_state or st.session_state["pull_data"]:
    # Automatically pull the data from HuggingFace if we're running on
    # Streamlit Community Cloud, as it doesn't seem to provide a separate
    # customizable setup step.
    #
    # This step does nothing when run in a local environment.
    automatically_pull_data_if_needed()
    st.session_state["pull_data"] = False


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


# makes it so streamlit doesn't have to reload for every sesson.
@st.cache_resource
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
    return (pl.scan_csv('processed_data/data_notes.csv')
            .rename({'Artist': Track.artist_names, 'Song': Track.name})
            .with_columns(pl.col([Track.name, Track.artist_names]).cast(pl.Categorical))
            )


@st.cache_data
def load_countries():
    return load_search_engine().data.countries


@st.cache_data
def load_stats():
    return load_search_engine().get_stats()


# Initialize session state
if "processing" not in st.session_state:
    st.session_state["processing"] = False

search_engine = load_search_engine()
df_notes = load_notes()
countries = load_countries()
songs_count, artists_count, playlists_count, djs_count, lyrics_count = load_stats()


# st.write(f"Memory Usage: {psutil.virtual_memory().percent}%")
st.markdown("## Westie Music Database:")
# byebye memory problems courtesy of Lukas W
st.text("An aggregated collection of West Coast Swing (WCS) music and playlists from DJs, Spotify users, etc. ")

# st.markdown('''468,348 **Songs** (160,661 wcs specific)
# 124,957 **Artists** (53,789 wcs specific)
# 54,005 **Playlists** (17,274 wcs specific)
# 1,298 **Westies/DJs**''')

st.write(f"{songs_count:,}   Songs")
st.write(f"{artists_count:,}   Artists")
st.write(f"{playlists_count:,}   Playlists")
st.write(f"{djs_count:,}   Westies/DJs\n\n")


st.link_button("Help fill in country info!",
               url='https://docs.google.com/spreadsheets/d/1YQaWwtIy9bqSNTXR9GrEy86Ix51cvon9zzHVh7sBi0A/edit?usp=sharing')


# Feature flag to enable the "Random Song" section
enable_random_song = False

if enable_random_song:
    st.markdown(f"#### ")
    st.markdown(f"#### Random Song")

    st.dataframe(search_engine.find_random_songs(playlist_count_range=(20, 800),
                                                 dj_count_range=(20, 300),
                                                 limit=1)
                 .select(Track.name, Track.artists, Track.url),
                 column_config={Track.url: st.column_config.LinkColumn()})

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
#                  column_config={Track.url: st.column_config.LinkColumn(),
#                                 "playlist_url": st.column_config.LinkColumn(),
#                                 "owner_url": st.column_config.LinkColumn()})
#     st.markdown(f"#### ")


st.markdown("#### ")
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
        .rename({Track.country: 'country'})\
        .drop(Track.region)\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .select(pull_columns_to_front(
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
        ))\
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
        .rename({Track.country: 'country'})\
        .drop(Track.region)\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .select(pull_columns_to_front(
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
        ))\
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
        .rename({Track.country: 'country'})\
        .drop(Track.region)\
        .select((cs.all()
                - Playlist.matching_columns()
                - PlaylistTrack.matching_columns()
                - PlaylistOwner.matching_columns())
                | cs.by_name(Playlist.name)
                | cs.by_name(PlaylistOwner.name))\
        .select(pull_columns_to_front(
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
        ))\
        .with_row_index(offset=1)\
        .collect(engine='streaming')


top_songs_toggle = st.toggle("Top 100 WCS songs!")
if top_songs_toggle:
    top_songs = top_songs()
    st.markdown(f"Top 100 WCS songs!")
    st.link_button('Playlist of the top 100',
                   url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_songs.drop(Stats.playlist_count),
                 column_config={Track.url: st.column_config.LinkColumn()})

    st.markdown("Top 100 🏳️‍🌈 songs!")
    top_queer_songs = top_queer_songs()

    # st.link_button('Playlist of the top 100',
    #        url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_queer_songs.drop(Stats.playlist_count),
                 column_config={Track.url: st.column_config.LinkColumn()})

    st.markdown("Top 100 POC songs!")
    top_poc_songs = top_poc_songs()

    # st.link_button('Playlist of the top 100',
    #        url='https://open.spotify.com/playlist/7f5hPmFnIPy7lcj8EXX90V')

    st.dataframe(top_poc_songs.drop(Stats.playlist_count),
                 column_config={Track.url: st.column_config.LinkColumn()})


# Courtesy of Vishal S.
song_locator_toggle = st.toggle("Find a Song 🎵")
if song_locator_toggle:
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
        bpm_slider = st.slider("Search BPM:", 0, 150, (0, 150))

    if not countries_selectbox:
        countries_2_filter = countries
    if countries_selectbox:
        countries_2_filter = countries_selectbox

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        bpm_low = st.number_input(
            "Playlist low: ", value=90, min_value=0, step=2)
    with col2:
        bpm_med = st.number_input(
            "Playlist med: ", value=95, min_value=0, step=2)
    with col3:
        bpm_high = st.number_input(
            "Playlist high: ", value=100, min_value=0, step=2)

    if st.button("Search songs", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True
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
            limit=1000,
        )

        results_df = song_search_df\
            .with_columns(
                pl.col(Playlist.name).list.head(30),
            )\
            .rename({Track.country: 'country'})\
            .drop(Track.id, Track.release_date, Track.region,
                  Playlist.matched_terms_count)\
            .select((cs.all()
                    - Playlist.matching_columns()
                    - PlaylistTrack.matching_columns()
                    - PlaylistOwner.matching_columns())
                    | cs.by_name(Playlist.name)
                    | cs.by_name(PlaylistOwner.name))\
            .select(pull_columns_to_front(
                Track.name,
                Track.url,
                Stats.playlist_count,
                Stats.dj_count,
                'hit_terms',
                Track.beats_per_minute,
                'matching_playlist_count',
                Track.has_queer_artist,
                Track.has_poc_artist,
                Playlist.name,
                Track.artists,
                PlaylistOwner.name,
                'country',
            ))\
            .with_row_index(offset=1)\
            .collect(engine="streaming")

        st.dataframe(results_df,
                     column_config={Track.url: st.column_config.LinkColumn()})

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
        # if st.button("Generate a playlist?", type="primary", disabled=st.session_state["processing"]):
        # st.session_state["processing"] = True
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
                     column_config={Track.url: st.column_config.LinkColumn()})

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
        # column_config={Track.url: st.column_config.LinkColumn("Song")}
        # )
        st.session_state["processing"] = False

    st.markdown(f"#### ")


# Courtesy of Vishal S.
playlist_locator_toggle = st.toggle("Find a Playlist 💿")
if playlist_locator_toggle:
    playlist_col1, playlist_col2 = st.columns(2)
    with playlist_col1:
        song_and_artist_input = st.text_input("Contains the song (use `song|artist` to filter by artist):")
        playlist_input = st.text_input("Playlist name:")
    with playlist_col2:
        dj_input = st.text_input("DJ name:")
        anti_playlist_input2 = st.text_input("Not in playlist name: ")

    song_and_artist_input = song_and_artist_input.split("|")
    song_input = song_and_artist_input[0] if len(song_and_artist_input) > 0 else ''
    artist_input = song_and_artist_input[1] if len(song_and_artist_input) > 1 else ''

    # if any(val for val in [playlist_input, song_input, dj_input]):
    if st.button("Search playlists", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True
        log_query("Search playlists", {'song_input': song_input,
                                       'dj_input': dj_input,
                                       'playlist_input': playlist_input,
                                       'anti_playlist_input': anti_playlist_input2,
                                       })

        # 420 owners have enough playlists to uniquely identify their date style
        # 593 owners have playlists with dates
        # leaving 593 - 420 = 173 owners with still ambigous date styles

        # If playlists contain references to weekdays, we can also discard
        # dates that do not match the given weekday, to further narrow down
        # the likely format of the playlist.

        # Also, we can use metadata like the creation date of the playlist
        # to narrow down the year / discard interpretations that do not seem
        # to match year indicated by the metadata +/- 1

        # Additionally, to decide between day vs. month first, we can use
        # statistical analysis, to see which field has the smaller distribution
        # of numbers - that is likely to be the year field.

        unambiguous = search_engine.find_date_formats_by_dj(only_unique_date_formats=True).collect()

        owner_date_formats_df = search_engine.find_date_formats_by_dj(
            # country=...,
            dj_name=dj_input,
            dj_exclude_by_ids=unambiguous,
            playlist_include=playlist_input,
            playlist_exclude=anti_playlist_input2,
            sort_by=Stats.date_format_counts,
            descending=True,
            limit=None,
        )

        print(owner_date_formats_df)

        st.dataframe(owner_date_formats_df.collect(engine='streaming'),
                     column_config={Playlist.url: st.column_config.LinkColumn()})

        # TODO: Expose additional query parameters in the UI
        playlist_search_df = search_engine.find_playlists(
            song_name=song_input,
            artist_name=artist_input,
            # country=...,
            dj_name=dj_input,
            playlist_include=playlist_input,
            playlist_exclude=anti_playlist_input2,
            tracks_in_result=True,
            tracks_limit=30,
            sort_by=[
                Playlist.matched_terms_count,
                Playlist.matching_song_count,
                Stats.song_count,
                Stats.artist_count,
            ],
            descending=True,
            limit=500,
        )

        st.dataframe(playlist_search_df
                     .select(Playlist.name, Playlist.date_types, Playlist.url, PlaylistOwner.name,
                             Playlist.matching_song_count, Stats.song_count,
                             Stats.artist_count, Track.name)
                     .collect(engine='streaming'),
                     column_config={Playlist.url: st.column_config.LinkColumn()})

        st.session_state["processing"] = False
    st.markdown(f"#### ")


@st.cache_data
def tags_data():
    return search_engine.find_tags(limit=1000, playlist_limit=20)\
        .collect(engine='streaming')


keyword_insights_toggle = st.toggle("Tag Insights 🏷️")

if keyword_insights_toggle:
    st.markdown(f"\n\n\n#### Common Tags for Playlists:")
    st.text(f"Disclaimer: Insights are based on a manually defined list of tags and aliases that is then used to extract keywords from playlist titles, and may not be accurate or representative of reality.")

    tags_df = tags_data()

    categories = tags_df.lazy()\
        .select(Tag.category)\
        .filter(Tag.category().is_not_null())\
        .unique()\
        .sort(Tag.category)\
        .collect()[Tag.category].to_list()

    ALL_CATEGORIES: Final = "(All categories)"
    tag_category_input = st.selectbox("Only show tags in category:", options=[ALL_CATEGORIES, *categories],
                                      format_func=lambda category: category.title() if category != ALL_CATEGORIES else category)

    show_wordcloud = st.toggle("Show wordcloud")

    if tag_category_input == ALL_CATEGORIES:
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

    dark_colors = [
        "#ffd16a",  # (1) Light Orange [orange50]
        "#faca2b",  # (1) Light Orange [lightTheme.yellowColor]
        "#803df5",  # (2) Violet [lightTheme.violetColor]~
        "#00c0f2",  # (3) Turqouise~
        "#83c9ff",  # (4) Light Blue [blue40]
        "#29b09d",  # (5) Blue-Green [blueGreen80]~
        "#ffabab",  # (6) Light Red [red40]
        "#7defa1",  # (7) Light Green [green40]
        "#d5dae5",  # (8) Light Gray [gray40]
    ]
    light_colors = [
        "#ffa421",  # (1) Orange [lightTheme.orangeColor]
        "#803df5",  # (2) Violet [lightTheme.violetColor]
        "#00c0f2",  # (3) Turqouise
        "#0068c9",  # (4) Dark Blue [blue80]
        "#29b09d",  # (5) Blue-Green [blueGreen80]
        "#ff2b2b",  # (6a) Medium Red [red80]
        # "#ff4b4b", # (6b) Red [lightTheme.redColor]
        "#21c354",  # (7) Green [lightTheme.greenColor]
        "#a3a8b8",  # (8) Gray [lightTheme.grayColor]
    ]
    base_colors = dark_colors

    # Cycle through the a predefined list of colors by default
    category_colors = (base_colors * int(math.ceil(len(categories) / len(base_colors))))[:len(categories)]

    # Allow overriding the color for specific categories
    customized_category_colors = load_keyword_colors()
    for i in range(0, len(categories)):
        category = categories[i]
        if category in customized_category_colors:
            category_colors[i] = customized_category_colors[category]

    # Create a mapping from category name => color
    color_by_category = {categories[i]: category_colors[i] for i in range(0, len(categories))}

    # Assign colors to tags based on their category
    tags = []
    full_tags = []
    tag_colors = []

    for row in tags_df.filter(pl.col('tag').is_not_null()).sort('full_tag').select('category', 'tag', 'full_tag').iter_rows():
        tags.append(row[1])
        full_tags.append(row[2])
        tag_colors.append(color_by_category[row[0]])

    st.dataframe(filtered_tags_df, column_config={
                 'category': st.column_config.MultiselectColumn(None, options=categories, color=category_colors),
                 'tag': st.column_config.MultiselectColumn(None, options=tags, color=tag_colors),
                 'full_tag': st.column_config.MultiselectColumn(None, options=full_tags, color=tag_colors),
                 })

    st.markdown(f"#### ")
    st.markdown(f"#### Tagged songs & playlists")

    UNTAGGED = "untagged"
    tag_input = st.selectbox("Show playlists & songs with tag:", options=[UNTAGGED, *full_tags],
                             format_func=lambda tag: ': '.join(tag.split(':')).title() if tag != UNTAGGED else "(Untagged)")

    if tag_input:
        st.markdown(f"Playlists tagged with _{tag_input}_:")

        tagged_playlists_df = search_engine\
            .find_playlists(tag_include=[tag_input])\
            .with_row_index(offset=1)

        st.dataframe(tagged_playlists_df)

        st.markdown(f"Songs tagged with _{tag_input}_:")

        tagged_songs_df = search_engine\
            .find_songs_by_tag(tag_name_exact=tag_input)\
            .with_row_index(offset=1)\
            .collect(engine='streaming')

        st.dataframe(tagged_songs_df.select(Track.name,
                                            Track.artists,
                                            TrackTag.tag,
                                            TrackTag.matching_playlist_count,
                                            TrackTag.Tag.playlist_percent,
                                            TrackTag.Tag.playlist_count,
                                            TrackTag.Track.playlist_percent,
                                            TrackTag.Track.playlist_count),
                     column_config={TrackTag.tag: st.column_config.MultiselectColumn(None, options=full_tags, color=tag_colors),
                                    TrackTag.matching_playlist_count: st.column_config.NumberColumn('#'),
                                    TrackTag.Tag.playlist_count: st.column_config.NumberColumn('# tag'),
                                    TrackTag.Tag.playlist_percent: st.column_config.ProgressColumn('% tag'),
                                    TrackTag.Track.playlist_count: st.column_config.NumberColumn('# track'),
                                    TrackTag.Track.playlist_percent: st.column_config.ProgressColumn('% track')})

        tagged_songs_df = tagged_songs_df\
            .limit(500)\
            .select(pl.all().name.map(lambda x: x.replace('.', '_')))

        st.bar_chart(tagged_songs_df, x='index', y='matching_playlist_count', sort=False)
        st.bar_chart(tagged_songs_df, x='index', y='tag_playlist_percent', sort=False)
        st.bar_chart(tagged_songs_df, x='index', y='track_playlist_count', sort=False)
        st.bar_chart(tagged_songs_df, x='index', y='track_playlist_percent', sort=False)


@st.cache_data
def djs_data():
    return load_search_engine().get_dj_stats(playlist_limit=30, dj_limit=2000).collect(engine='streaming')


# Courtesy of Lino V.
search_dj_toggle = st.toggle("DJ insights 🎧")

if search_dj_toggle:
    dj_col1, dj_col2 = st.columns(2)
    with dj_col1:
        dj_input = st.text_input("DJ name/ID (ex. Kasia Stepek or 1185428002)")
    with dj_col2:
        dj_playlist_input = st.text_input("DJ playlist name:")

    if not dj_input and not dj_playlist_input:
        djs_data = djs_data()
        st.dataframe(djs_data,
                     column_config={PlaylistOwner.url: st.column_config.LinkColumn()})

    # else:
    if st.button("Search djs", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True
        log_query("Search djs", {'dj_input': dj_input,
                                 'dj_playlist_input': dj_playlist_input,
                                 })

        dj_search_df = search_engine.find_djs(
            dj_name=dj_input,
            playlist_name=dj_playlist_input,
            dj_limit=100,
            playlist_limit=30,
        ).collect(engine='streaming')

        st.dataframe(dj_search_df,
                     column_config={PlaylistOwner.url: st.column_config.LinkColumn()})

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
                         column_config={Track.url: st.column_config.LinkColumn()})

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
            #         column_config={Track.url: st.column_config.LinkColumn()})
        st.session_state["processing"] = False

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

    if st.button("Compare DJs/users", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True
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
                     column_config={Track.url: st.column_config.LinkColumn()})
        st.session_state["processing"] = False
    st.markdown(f"#### ")


@st.cache_data
def region_data():
    return (search_engine.get_region_stats()
            .collect(engine='streaming'))


@st.cache_data
def country_data():
    return (search_engine.get_country_stats()
            .collect(engine='streaming'))


# Courtesy of Lino V.
geo_region_toggle = st.toggle("Geographic Insights 🌎")
if geo_region_toggle:
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

        st.dataframe(region_df.head(1000).collect(engine='streaming'),
                     column_config={Track.url: st.column_config.LinkColumn()})

    st.markdown(f"#### Comparing Countries' music:")
    countries_selectbox = st.multiselect(
        "Compare these countries' music:",
        countries,
        max_selections=2,
    )

    if st.button("Compare countries", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True
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
        st.dataframe(compare_df.collect(engine='streaming'),
                     column_config={Track.url: st.column_config.LinkColumn()})
        st.session_state["processing"] = False
        st.markdown(f"#### ")


@st.cache_data
def top_related_songs():
    return (search_engine.find_related_songs('any', return_pairs=True, limit=1000)[1]
            .select(TrackAdjacent.FirstTrack.name, TrackAdjacent.FirstTrack.artists,
                    TrackAdjacent.times_played_together,
                    TrackAdjacent.SecondTrack.name, TrackAdjacent.SecondTrack.artists)
            .collect(engine='streaming'))


# Courtesy of Vincent M.
songs_together_toggle = st.toggle("Songs most played together")

if songs_together_toggle:

    song_combo_col1, song_combo_col2 = st.columns(2)
    with song_combo_col1:
        song_input = st.text_input("Song Name:")
    with song_combo_col2:
        artist_name_input = st.text_input("Song artist name:")

    if not song_input and not artist_name_input:
        st.markdown("#### Most common songs to play next to each other")
        top_related_songs = top_related_songs()
        st.dataframe(top_related_songs,
                     column_config={Track.url: st.column_config.LinkColumn()})

    if st.button("Search songs played together", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True

        st.markdown("#### Songs"
                    + (f" matching _{song_input}_" if song_input else "")
                    + (f" by _{artist_name_input}_" if artist_name_input else "")
                    + ":")
        st.dataframe(search_engine.find_songs(song_name=song_input, artist_name=artist_name_input, limit=100)
                     .select(Track.name, Track.artists, Track.url,
                             Track.beats_per_minute, Track.release_date),
                     column_config={Track.url: st.column_config.LinkColumn()})

        st.markdown(f"#### Most common songs to play after _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('next', song_name=song_input, artist_name=artist_name_input)[1]
                     .select(Track.name, Track.artists, TrackAdjacent.times_played_together,
                             Track.url, Track.beats_per_minute, Track.release_date),
                     column_config={Track.url: st.column_config.LinkColumn()})

        st.markdown(f"#### Most common songs to play before _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('prev', song_name=song_input, artist_name=artist_name_input)[1]
                     .select(Track.name, Track.artists, TrackAdjacent.times_played_together,
                             Track.url, Track.beats_per_minute, Track.release_date),
                     column_config={Track.url: st.column_config.LinkColumn()})

        st.markdown(f"#### Most common songs to play before _or_ after _{song_input}_:")
        st.dataframe(search_engine.find_related_songs('any', song_name=song_input, artist_name=artist_name_input)[1]
                     .select(Track.name, Track.artists, TrackAdjacent.times_played_together,
                             Track.url, Track.beats_per_minute, Track.release_date),
                     column_config={Track.url: st.column_config.LinkColumn()})

        st.session_state["processing"] = False
    st.link_button("Andreas' connected-songs visualization!",
                   'https://loewclan.de/song-galaxy/')
    st.markdown(f"#### ")


@st.cache_data
def songs_by_year():
    current_year: Final = time.localtime().tm_year
    return search_engine.get_popularity_over_time(interval='year', year_range=(2000, current_year))\
        .collect(engine='streaming')


song_popularity_toggle = st.toggle("Song popularity over time 📊")
if song_popularity_toggle:
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

    search_button = st.button("Show song popularity over time", type="primary", disabled=st.session_state["processing"])

    popularity_df: pl.DataFrame | None = None
    is_search_result: bool = False

    if not song_input and not artist_name_input and not search_button:
        popularity_df = songs_by_year()
        interval_input = 'year'

    if search_button:
        st.session_state["processing"] = True
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

        st.session_state["processing"] = False

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

lyrics_toggle = st.toggle("Search lyrics 📋")
if lyrics_toggle:

    st.write(f"from {lyrics_count:,} songs")
    lyrics_col1, lyrics_col2 = st.columns(2)
    with lyrics_col1:
        song_input = st.text_input("Song:")
        lyrics_input = st.text_input("In lyrics:")

    with lyrics_col2:
        artist_input = st.text_input("Artist:")
        anti_lyrics_input = st.text_input("Not in lyrics:")

    if st.button("Search lyrics", type="primary", disabled=st.session_state["processing"]):
        st.session_state["processing"] = True

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
            column_config={Track.url: st.column_config.LinkColumn()})

        st.session_state["processing"] = False


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
