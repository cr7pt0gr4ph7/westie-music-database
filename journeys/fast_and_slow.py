import polars as pl
import streamlit as st

from journeys.journey_utils import _count, count, dataset_note, percentage, song_tag_link, render_examples, search_engine
from utils.tables import Track, TrackTag, TrackTags

st.set_page_config(
    page_title="What's fast?",
    page_icon=":material/acute:",
)

st.title("What is considered _fast_?")

dataset_note()

tag_link = song_tag_link

f"""
Our dataset contains both the objective tempo in Beats per Minute (BPM) for a sizeable subset of tracks
:small[({count('songs', with_bpm=True)} tracks,
or {percentage('songs', with_bpm=True)} of our dataset)],
as well as the information on how often a given track has been tagged as
{tag_link('tempo:fast')} and/or {tag_link('tempo:slow')}
:small[({count('songs', with_any_tag=['tempo:fast', 'tempo:slow'])} tracks,
or {percentage('songs', with_any_tag=['tempo:fast', 'tempo:slow'])} of our dataset)].
"""

render_examples("1")

f"""
Correlating these two measures for the subset of tags that contain both
:small[({count('songs', with_bpm=True, with_any_tag=['tempo:fast', 'tempo:slow'])} tracks,
or {percentage('songs', with_bpm=True, with_any_tag=['tempo:fast', 'tempo:slow'])} of our dataset)]
allows us to get an insight of how the subjective measure of _"fastness/slowness"_
correlates with the objective measure of BPM.
"""


@st.fragment
def table_song_tempo_vs_fast_and_slow_tags():
    rolling_average_window = 5

    df = search_engine\
        .find_songs(song_has_bpm=True,
                    tag_include=['tempo:fast', 'tempo:slow'])\
        .select(pl.concat_str(Track.name, pl.lit(' \u2013 '), Track.artist_names).alias('track_title'),
                Track.beats_per_minute().cast(pl.UInt32).alias('bpm'),
                TrackTags.tags_data()
                .list.filter(TrackTag.tag.struct_field().eq('tempo:fast'))
                .list.eval(TrackTag.matching_playlist_count.struct_field())
                .list.first().fill_null(0)
                .alias('tag_fast_count'),
                TrackTags.tags_data()
                .list.filter(TrackTag.tag.struct_field().eq('tempo:medium'))
                .list.eval(TrackTag.matching_playlist_count.struct_field())
                .list.first().fill_null(0)
                .alias('tag_medium_count'),
                TrackTags.tags_data()
                .list.filter(TrackTag.tag.struct_field().eq('tempo:slow'))
                .list.eval(TrackTag.matching_playlist_count.struct_field())
                .list.first().fill_null(0)
                .alias('tag_slow_count'))\
        .filter((pl.col('tag_fast_count') + pl.col('tag_medium_count') + pl.col('tag_slow_count')).gt(20))\
        .group_by('bpm')\
        .agg(pl.col('tag_fast_count', 'tag_medium_count', 'tag_slow_count').mean())\
        .with_columns((pl.col('tag_fast_count') / pl.col('tag_slow_count')).alias('tag_ratio'),
                      (pl.col('tag_fast_count') / (pl.col('tag_fast_count') +
                       pl.col('tag_slow_count'))).alias('tag_fast_percent'),
                      (pl.col('tag_slow_count') / (pl.col('tag_fast_count') + pl.col('tag_slow_count'))).alias('tag_slow_percent'))\
        .sort('bpm')\
        .with_columns(pl.col('tag_fast_count', 'tag_medium_count', 'tag_slow_count')
                      .rolling_median(rolling_average_window, min_samples=1, center=True)
                      .name.suffix('_median'),
                      pl.col('tag_fast_count', 'tag_medium_count', 'tag_slow_count')
                      .rolling_mean(rolling_average_window, min_samples=1, center=True)
                      .name.suffix('_mean'))

    total = _count('songs', with_bpm=True, with_any_tag=['tempo:fast', 'tempo:slow'])
    num_samples = 100

    # df = df.gather_every(total // num_samples)

    df = df.collect()

    with st.expander("Raw data", expanded=False):
        st.bar_chart(df,
                     x='bpm',
                     y=['tag_slow_count', 'tag_medium_count', 'tag_fast_count'],
                     x_label='Beats per Minute (BPM)',
                     y_label=['# tag:slow', '# tag:fast'],
                     stack=False)

    with st.expander(f"Rolling mean (window size = {rolling_average_window})", expanded=True):
        st.line_chart(df,
                      x='bpm',
                      y=['tag_slow_count_mean', 'tag_medium_count_mean', 'tag_fast_count_mean'],
                      x_label='Beats per Minute (BPM)')

    with st.expander(f"Rolling median (window size = {rolling_average_window})", expanded=False):
        st.bar_chart(df,
                     x='bpm',
                     y=['tag_slow_count_median', 'tag_medium_count_median', 'tag_fast_count_median'],
                     x_label='Beats per Minute (BPM)',
                     stack=False)

    with st.expander(f"Percentage of total", expanded=False):
        st.bar_chart(df,
                     x='bpm',
                     y=['tag_slow_percent', 'tag_fast_percent'],
                     x_label='Beats per Minute (BPM)',
                     stack=False)


table_song_tempo_vs_fast_and_slow_tags()
