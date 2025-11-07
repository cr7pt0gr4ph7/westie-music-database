import math
import random

import polars as pl
import streamlit as st

from journeys.journey_utils import _count, base_column_config, count, dataset_note, percentage, playlist_name_like, render_examples, search_engine, tag_link
from utils.common.filters import create_text_filter
from utils.playlist_classifiers import contains_bpm_in_name, contains_date_in_name, contains_month_year_in_name
from utils.tables import Playlist, PlaylistOwner, PlaylistStats, PlaylistTags, Stats

st.set_page_config(
    page_title="About Tagging",
    page_icon=":material/sell:",
)

st.title("How we tag songs (in a way that is _actually_ useful)", "main")

f"""
#### _How to: Tagging songs based on messy data_

This page tells the story of how we tag songs based on the large but
at the same time limited dataset that we have \u2014 and the tricks we
employ to make this work.
"""

dataset_note()

#
# MISSION STATEMENT
#

"""
## Why?

###### What are we trying to achieve?

While it is nice & enlightening to be able to compare countries' music / trends in how & where a song is played / ...
my immediate question as an individual DJ (and dance teacher) was instead:

_»How can I use this to discover_ **new music** _I do_ **not yet know** _but which_ **fits the purpose**
_I'm trying to achieve,_ **without** _the cool new stuff_ **being drowned out in the noise**_?«_
"""

#
# FROM TAGGING PLAYLISTS....
#

st.header("From tagging playlists...")

f"""
The first step in our journey is pretty easily explained: Tag playlists with
tags from a limited set of predefined tags, based purely on the playlist names.

This way, you can search for playlists tagged with {tag_link('late night', tag='genre:late night', code=True)},
and also get playlists named {playlist_name_like('late nite')},
{playlist_name_like("latenight")}, {playlist_name_like('late nights')} etc.,
as well as equivalent terms in other languages.
"""

render_examples("1")

f"""
Tags are also grouped into different categories as a convenience for users,
so we have {tag_link('genre:blues')} and {tag_link('genre:pop')},
but also  {tag_link('mood:chill')} and {tag_link('mood:high energy')}.
"""

render_examples("2")

"""
The list of keywords and associated tags is already quite large,
and can be [:small[:material/open_in_new:] found in][kw-data] `utils/keyword_data.yaml` [on GitHub][kw-data].

[kw-data]: https://github.com/ThomasMAhern/westie-music-database
"""

#
# ...TO TAGGING SONGS
#

st.header("...to tagging songs")

f"""
Tagging playlists in this way works pretty well, so the next logical step is to ask the question:
_»Can we use the tagging information from the playlists to automatically derive sets of tags for songs?«_.
"""

with st.container(border=True):
    """
    :blue-badge[:material/info: Details]

    More precisely, for any given `(song, tag)` pairing, we want to estimate the following probability:

    > _»How likely is it that one of the DJs from our dataset would consider song X to be a WCS / late night / ... song?«_.

    That is, if we played a given song to 100 DJs, how many would agree it's a late night song?
    Though this is obviously a simplification, as we are dealing with random and uncertain statistical data.
    """

song_tags = [
    'epoch',
    'genre',
    'language',
    'mood',
    'tempo',
    'timing',
    'topic',
]

f"""
In a first step, simply counting the number of e.g. `genre:late night` playlists a song appears in yields acceptable results.

We run into two problems:

1.  Only a small percentage \u2014 roughly {percentage('playlists', with_any_tag=song_tags)}
    \u2014 of our playlists have names from which we can derive useful information about the songs they contain.

    Another {percentage('playlists', with_any_tag=['context:social', 'events'])} of playlist are named in a way
    that indicates they are a class/workshop/event playlist.
    - Late night, late nite, ...
    - Class/Social/...

2.  While a few popular songs are contained in many playlists, most of the songs we are actually
    interested in are contained in comparatively fewer playlists

    If we plot the number of playlists per song, it quickly becomes apparent that we have a typical
    :blue-badge[:material/open_in_new: [long-tail distribution](https://www.google.com/search?q=long+tail+distribution)]
    with a very long tail:
"""

"""
The number playlists a song is contained in follows a classical long-tail distribution:
Popular songs are found in many playlists.

- Names do not always describe content
- we do not want to flat-out exclude uncommon tracks

- we care more about false positives (e.g. when {song_link('Uptown Funk', 'Bruno Mars')}) is tagged as a
- false positives/false negatives

- Playlists that contain WCS in their name

- Broad vs. narrow use
- Mistagging
- big dump playlists
"""

st.header("Understanding our dataset")

st.subheader("Classifiying playlists")


def classify_playlist() -> pl.Expr:
    playlist_name = Playlist.name()
    has_date = contains_date_in_name(playlist_name)
    has_month_year = contains_month_year_in_name(playlist_name)
    has_bpm = contains_bpm_in_name(playlist_name)

    playlist_tags = PlaylistTags.tags()
    has_tag = lambda *tags: create_text_filter(
        tags, playlist_tags, match_mode='exact|category', is_list_column=True)

    has_song_tags = has_tag(*song_tags)
    has_dance = has_tag('dance')
    has_wcs_tag = has_tag('dance:wcs')
    has_artist = has_tag('artist')
    has_event = has_tag('events')
    has_context = has_tag('context')
    has_social_tag = has_tag('context:social')
    has_class_tag = has_tag('context:class')
    has_competition_tag = has_tag('context:competition')
    has_seasonal_tag = has_tag('seasonal')
    is_shazam_playlist = has_tag('playlist:shazam')
    is_personal_favorites = has_tag('playlist:favorites')
    is_ordered = has_tag('playlist:ordered')
    has_level = has_tag('level')

    return pl.concat_list([
        pl.when(has_tag('weekday')).then(pl.lit('x:weekday')),
        pl.when(has_tag('month')).then(pl.lit('x:month')),
        pl.when(has_date).then(pl.lit('date'))
          .when(has_month_year).then(pl.lit('month')),
        pl.when(has_bpm).then(pl.lit('bpm')),
        pl.when(has_artist).then(pl.lit('artist')),
        pl.when(has_song_tags).then(pl.lit('song_tags')),
        pl.when(has_social_tag).then(pl.lit('social')),
        pl.when(has_class_tag).then(pl.lit('class')),
        pl.when(has_competition_tag).then(pl.lit('competition')),
        pl.when(~has_social_tag, ~has_class_tag, ~has_competition_tag, has_context).then(pl.lit('context')),
        pl.when(has_event).then(pl.lit('event')),
        pl.when(has_dance).then(pl.lit('dance')),
        pl.when(has_wcs_tag).then(pl.lit('wcs')),
        pl.when(is_shazam_playlist).then(pl.lit('shazam')),
        pl.when(is_personal_favorites).then(pl.lit('favs')),
        pl.when(has_seasonal_tag).then(pl.lit('seasonal')),
        pl.when(is_ordered).then(pl.lit('ordered')),
        pl.when(has_tag('misc', 'miscx')).then(pl.lit('misc')),
        pl.when(has_tag('playlist:unrelated', 'playlist:spotify stuff')).then(pl.lit('misc')),
        pl.when(has_level).then(pl.lit('level')),
    ]).list.drop_nulls()


@st.fragment
def table_playlist_classification():
    total = _count('playlists')
    limit = 100
    offset = math.floor(random.random() * (limit-1))

    groups_df = search_engine\
        .find_playlists()\
        .with_columns(classify_playlist().alias('classification'))\
        .group_by('classification')\
        .agg(Playlist.id().count().alias(Stats.playlist_count))\
        .with_columns((Stats.playlist_count() / total).alias(Stats.playlist_percent))\
        .sort(Stats.playlist_count, descending=True)

    playlists_df = search_engine\
        .find_playlists()\
        .with_columns(classify_playlist().alias('classification'))\
        .filter(pl.col('classification').list.len().eq(0))\
        .sort(Stats.song_count, descending=True)\
        .select(Playlist.name, PlaylistOwner.name, PlaylistTags.tags, Stats.song_count, PlaylistStats.wcs_song_percent, Playlist.url)\
        .slice(0, 1000)

    groups_df, playlists_df = pl.collect_all((groups_df, playlists_df), engine='streaming')

    st.markdown("###### :blue-badge[Interactive] Playlists grouped by classification")

    st.dataframe(groups_df, column_config=base_column_config)

    st.markdown("###### :blue-badge[Interactive] Unclassified Playlists")

    st.dataframe(playlists_df, column_config=base_column_config | {
                 Playlist.name: st.column_config.TextColumn(pinned=True)})


table_playlist_classification()


st.subheader("Useful keywords in playlist names")

f"""
#### The term `WCS` and common variations like `West Coast Swing`, `Westie` etc.

We have a large number of playlists :small[({count('playlists', with_tag='dance:wcs')}
playlists, or ~{percentage('playlists', with_tag='dance:wcs')} of our dataset)]
that have `WCS` or some variation in their name.

While these playlists aren't enough to fully distinguish between `suitable for WCS`
and `Not WCS` songs, we can use these {count('songs', with_tag='dance:wcs')}
:small[(= ~{percentage('songs', with_tag='dance:wcs')} of all songs in our dataset)]
likely-to-be-WCS songs to estimate how likely a playlist is to contain other WCS songs.

It seems that a minimum threshold of somewhere between 0 and 10 % of WCS songs in a playlist
seems to work for weeding out non-WCS playlists.
"""


@st.fragment
def table_wcs_song_percent():
    total = _count('playlists')
    limit = 100
    offset = math.floor(random.random() * (limit-1))

    df = search_engine\
        .find_playlists(playlist_tag_exclude='dance:wcs',
                        playlist_stats_in_result=True,
                        tracks_in_result=False,
                        sort_by=PlaylistStats.wcs_song_percent,
                        descending=True,
                        limit=None)\
        .gather_every(total // limit, offset)\
        .select(Playlist.name, PlaylistOwner.name, Stats.song_count, PlaylistStats.wcs_song_percent, Playlist.url)\
        .limit(limit)

    st.markdown("###### :blue-badge[Interactive] Pseudo-random Sample chosen from our Dataset")

    st.dataframe(df, column_config=base_column_config)


table_wcs_song_percent()

f""""
One additional complication are combined `WCS/Zouk` playlists, which contain
both WCS and Zouk songs, where it isn't entirely clear whether all songs
are both `Zouk` and `WCS`. We currently tag these playlists as both `WCS` and `Zouk`.

#### The term `Late Night` and its variations

Surprisingly, one of the only other tags that is useful...

- Only used in the WCS context
- Not used anywhere else
- There is enough agreement on what "late night" means for it to be useful
&nbsp;
"""

"""

"""

"""
- Based on a manual examination of the data

We're working on the hypothesis that all playlists within our dataset
can be each sorted into one of three categories:


- Playlists of the music actually played at/in...
  - a competition
  - a class/workshop
  - a party

- Ordered playlists that

- BIG dumps of all WCS songs (EXAMPLES). The top  only make up ??? % of playlists in our dataset,
  but make up ??? & p
"""


"""
- playlist-wide cohesion
- local cohesion
    - ordered/sorted/increasing...
    - on which attributes?
    - hi/lo DJing styles:small[:material/open_in_new:]
- no cohesion
"""
