import streamlit as st
import wordcloud
import matplotlib.pyplot as plt
import polars as pl
import psutil

from utils.additional_data import actual_wcs_djs, poc_artists, queer_artists
from utils.common.logging import log_query
from utils.playlist_classifiers import extract_date_strings_from_name


# avail_threads = pl.threadpool_size()

pl.Config.set_tbl_rows(100).set_fmt_str_lengths(100)
pl.enable_string_cache()  # for Categoricals
# st.text(f"{avail_threads}")


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
def load_playlist_data():
    return (pl.scan_parquet('processed_data/data_playlists.parquet', low_memory=True)
            .rename({'name': 'playlist_name'})
            # makes a new column filled with a date - this is good indicator if there was a set played
            .with_columns(extracted_date=extract_date_strings_from_name(pl.col('playlist_name')).cast(pl.List(pl.Categorical)),
                          song_url=pl.when(pl.col('track.id').is_not_null())
                          .then(pl.concat_str(pl.lit('https://open.spotify.com/track/'), 'track.id')),
                          playlist_url=pl.when(
                              pl.col('playlist_id').is_not_null())
                          .then(pl.concat_str(pl.lit('https://open.spotify.com/playlist/'), 'playlist_id')),
                          owner_url=pl.when(pl.col('owner.id').is_not_null())
                          .then(pl.concat_str(pl.lit('https://open.spotify.com/user/'), 'owner.id')),
                          region=pl.col('location').str.split(
                              ' - ').list.get(0, null_on_oob=True),
                          country=pl.col('location').str.split(' - ').list.get(1, null_on_oob=True),)

            # gets the counts of djs, playlists, and geographic regions a song is found in
            .with_columns(dj_count=pl.n_unique('owner.display_name').over(['track.id', 'track.name']).cast(pl.UInt16),
                          playlist_count=pl.n_unique('playlist_name').over(
                              ['track.id', 'track.name']).cast(pl.UInt16),
                          regions=pl.col('region').over(
                              'track.name', mapping_strategy='join')
                          .list.unique()
                          .list.sort()
                          .list.join(', '),
                          countries=pl.col('country').over(
                              'track.name', mapping_strategy='join')
                          .list.unique()
                          .list.sort()
                          .list.join(', '),
                          song_position_in_playlist=pl.concat_str(pl.col('song_number'), pl.lit(
                              '/'), pl.col('tracks.total'), ignore_nulls=True),
                          actual_social_set=pl.when(pl.col('extracted_date').list.len().gt(0)
                                                    | pl.col('playlist_name').str.contains_any(['social', 'party', 'soir'],
                                                                                               ascii_case_insensitive=True))
                          .then(True)
                          .otherwise(False),
                          actual_wcs_dj=pl.when(pl.col('owner.id').str.contains_any(actual_wcs_djs, ascii_case_insensitive=True)
                                                | pl.col('owner.display_name').cast(pl.String).eq('Connie Wang')
                                                | pl.col('owner.display_name').cast(pl.String).eq('Koichi Tsunoda')
                                                )
                          .then(True)
                          .otherwise(False),
                          queer_artist=pl.when(
                              pl.col('track.artists.name').str.to_lowercase().is_in(queer_artists))
                          .then(True)
                          .otherwise(False),
                          poc_artist=pl.when(
                              pl.col('track.artists.name').str.to_lowercase().is_in(poc_artists))
                          .then(True)
                          .otherwise(False),
                          )
            .with_columns(
        #       apprx_song_position_in_playlist = pl.when((pl.col('actual_social_set').eq(True))
        #                                                       & ((pl.col('song_number') * 100 / pl.col('tracks.total')) <= 33))
        #                                                 .then(pl.lit('beginning'))
        #                                                 .when((pl.col('actual_social_set').eq(True))
        #                                                       & ((pl.col('song_number') * 100 / pl.col('tracks.total')) > 33)
        #                                                       & ((pl.col('song_number') * 100 / pl.col('tracks.total')) <= 66))
        #                                                 .then(pl.lit('middle'))
        #                                                 .when((pl.col('actual_social_set').eq(True))
        #                                                         & ((pl.col('song_number') * 100 / pl.col('tracks.total')) > 66))
        #                                                 .then(pl.lit('end')),
        geographic_region_count=pl.when(pl.col('regions').str.len_bytes() != 0)
        .then(pl.col('regions').str.split(', ').list.len())
        .otherwise(0),
    )
        .drop('regions', 'countries')
        # memory tricks
        .with_columns(pl.col('song_number', 'tracks.total').cast(pl.UInt16),
                      pl.col('geographic_region_count').cast(pl.Int8),
                      pl.col(['song_url', 'track.id', 'track.name', 'playlist_url', 'playlist_id', 'owner_url', 'song_position_in_playlist',
                              'track.artists.name',
                              #     'apprx_song_position_in_playlist',
                              'location', 'region', 'country', 'playlist_name', 'owner.display_name',
                              'owner.id',
                              ]).cast(pl.Categorical())
                      )
    )
# st.write(f"def is good")


def wcs_specific(df_):
    '''given a df, filter to the records most likely to be west coast swing related'''
    return (df_.lazy()
            .filter(pl.col('actual_social_set').eq(True)
                    | pl.col('actual_wcs_dj').eq(True)
                    | pl.col('playlist_name').cast(pl.String).str.contains_any(['wcs', 'social', 'party', 'soirée', 'west', 'routine',
                                                                               'practice', 'practise', 'westie', 'party', 'beginner',
                                                                                'bpm', 'swing', 'novice', 'intermediate', 'comp',
                                                                                'musicality', 'timing', 'pro show'], ascii_case_insensitive=True))
            )


@st.cache_resource
def load_lyrics():
    return pl.scan_parquet('processed_data/song_lyrics.parquet')


# makes it so streamlit doesn't have to reload for every sesson.
@st.cache_resource
def load_notes():
    return (pl.scan_csv('processed_data/data_notes.csv')
            .rename({'Artist': 'track.artists.name', 'Song': 'track.name'})
            .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical))
            )


@st.cache_data
def load_countries():
    return sorted(df.select(pl.col('country').cast(pl.String)).unique().drop_nulls().collect(streaming=True)['country'].to_list())


@st.cache_data
def load_stats():
    '''makes it so streamlit doesn't have to reload for every sesson/updated parameter
    should make it much more responsive'''
    stats_counts = (pl.scan_parquet('processed_data/data_playlists.parquet')
                    .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical))
                    .select(pl.n_unique('track.name'),
                            pl.n_unique('track.artists.name'),
                            pl.n_unique('name'),
                            pl.n_unique('owner.display_name'),
                            )
                    .collect(streaming=True)
                    .iter_rows()
                    )
    songs_count, artists_count, playlists_count, djs_count = list(stats_counts)[
        0]

    return songs_count, artists_count, playlists_count, djs_count


df = load_playlist_data()
# st.write(f"df is good")
df_lyrics = load_lyrics()
# st.write(f"lyrics is good")
df_notes = load_notes()
# st.write(f"notes is good")
countries = load_countries()
# st.write(f"countries is good")
songs_count, artists_count, playlists_count, djs_count = load_stats()
# st.write(f"stats is good")


# st.write(f"Memory Usage: {psutil.virtual_memory().percent}%")
st.markdown("## Westie Music Database:")
st.text("An aggregated collection of West Coast Swing (WCS) music and playlists from DJs, Spotify users, etc. (The free service I'm using is a delicate 🌷 with limited memory and may crash if queried multiple times before it can finish 🥲 )")

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

# st.markdown(f"#### ")


@st.cache_data
def sample_of_raw_data():
    return (df
            # .with_columns(pl.col('track.artists.name').cast(pl.String))
            .join(pl.scan_parquet('processed_data/data_song_bpm.parquet')
                  .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical)),
                  how='left', on=['track.name', 'track.artists.name'])
            # .with_columns(pl.col('track.artists.name').cast(pl.Categorical))
            ._fetch(100000).sample(500)
            )


sample_of_raw_data = sample_of_raw_data()

data_view_toggle = st.toggle("📊 Raw data")

if data_view_toggle:
    # num_records = st.slider("How many records?", 1, 1000, step=50)
    st.dataframe(sample_of_raw_data,
                 column_config={"song_url": st.column_config.LinkColumn(),
                                "playlist_url": st.column_config.LinkColumn(),
                                "owner_url": st.column_config.LinkColumn()})
    st.markdown(f"#### ")


st.markdown("#### ")
st.markdown("#### Choose your own adventure!")


@st.cache_data
def top_songs():
    '''creates the standard top songs until user '''
    return (df
            # add notes
            .join((df_notes
                   # .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical))
                   ),
                  how='full',
                  on=['track.artists.name', 'track.name'])
            # add bpm
            .join((pl.scan_parquet('processed_data/data_song_bpm.parquet')
                   .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical))
                   ), how='left', on=['track.name', 'track.artists.name'])

            .with_columns(pl.col('bpm').fill_null(pl.col('BPM')))
            .group_by('track.name', 'song_url', 'playlist_count', 'dj_count')
            .agg(pl.n_unique('playlist_name').alias('matching_playlist_count'), 'queer_artist', 'bpm',
                 'playlist_name', 'track.artists.name', 'owner.display_name', 'country', 'poc_artist',
                 #      'apprx_song_position_in_playlist',
                 'notes', 'note_source',
                 # connies notes
                 'Starting energy', 'Ending energy', 'BPM', 'Genres', 'Acousticness', 'Difficulty', 'Familiarity', 'Transition type')
            .with_columns(pl.col('playlist_name', 'owner.display_name',
                                 #      'apprx_song_position_in_playlist',
                                 'track.artists.name', 'country',
                                 # connies notes
                                 'Starting energy', 'Ending energy', 'queer_artist', 'bpm', 'BPM', 'Genres', 'Acousticness', 'Difficulty',
                                 'Familiarity', 'Transition type', 'poc_artist',
                                 ).list.unique().list.drop_nulls().list.head(50),
                          pl.col('notes', 'note_source').list.unique(
            ).list.sort().list.drop_nulls(),
            )
            .with_columns(pl.col("queer_artist").list.any(),  # resolves True/False to just True if any True are present
                          pl.col("poc_artist").list.any(),
                          )
            .select('track.name', 'song_url', 'playlist_count', 'dj_count', 'bpm',
                    pl.all().exclude('track.name', 'song_url', 'playlist_count', 'dj_count', 'bpm',))
            .sort('matching_playlist_count', descending=True)
            .with_row_index(offset=1)
            .head(500).collect(streaming=True)
            )


top_songs = top_songs()

top_songs_toggle = st.toggle("Top 500 WCS songs!")
if top_songs_toggle:
    st.dataframe(top_songs.drop('matching_playlist_count'),
                 column_config={"song_url": st.column_config.LinkColumn()}
                 )


# courtesy of Vishal S
song_locator_toggle = st.toggle("Find a Song 🎵")
if song_locator_toggle:
    song_col1, song_col2 = st.columns(2)
    with song_col1:
        song_input = st.text_input("Song name:").lower().split(',')
        artist_name = st.text_input("Artist name:").lower().split(',')
        dj_input = st.text_input("DJ/user name:").lower().split(',')
        playlist_input = st.text_input(
            "Playlist name ('late night', '80bpm', or 'Budafest'):").lower().split(',')
        queer_toggle = st.checkbox("🏳️‍🌈")
        poc_toggle = st.checkbox("POC")
        st.markdown(
            "[Add/correct POC artists](https://docs.google.com/spreadsheets/d/1-elrLd_3tX4QTLQjj4EmPxRSzXHcxs6tZp5Y5fRFalc/edit?usp=sharing)")

    with song_col2:
        countries_selectbox = st.multiselect("Country:", countries)
        added_2_playlist_date = st.text_input(
            "Added to playlist date (yyyy-mm-dd):").split(',')
        track_release_date = st.text_input(
            "Track release date (yyyy-mm-dd or '198' for 1980's music):").split(',')
        anti_playlist_input = st.text_input(
            "Exclude if in playlists ('blues', or 'zouk'):").lower().split(',')
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

    if queer_toggle:
        only_fabulous_people = queer_artists
    if not queer_toggle:
        only_fabulous_people = ['']

    if poc_toggle:
        only_poc_people = poc_artists
    if not poc_toggle:
        only_poc_people = ['']

    # if ''.join(anti_playlist_input).strip() == '':
    if anti_playlist_input == ['']:
        anti_playlist_input = [
            'this_is_a_bogus_value_to_hopefully_not_break_things']

    # if (song_input + artist_name + dj_input + ''.join(playlist_input) + ''.join(anti_playlist_input) +
    #     ''.join(countries_selectbox) + ''.join(added_2_playlist_date) + ''.join(track_release_date)
    #     ).strip() == 'this_is_a_bogus_value_to_hopefully_not_break_things' and num_results == 0 and not queer_toggle and bpm_slider[0]==0 and bpm_slider[1]==150:
    #         # st.text('preloaded')
    #         st.dataframe(top_songs,
    #                          column_config={"song_url": st.column_config.LinkColumn()}
    #                     )

    # else:
    if st.button("Search songs", type="primary"):

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

        # get all playlists a song is in
        anti_df = (df
                   .group_by('track.id')
                   .agg('playlist_name')
                   .explode('playlist_name')
                   .filter(pl.col('playlist_name').cast(pl.String).str.contains_any(anti_playlist_input,
                                                                                    ascii_case_insensitive=True))
                   .select('track.id')

                   )

        song_search_df = (
            df
            # .pipe(just_a_peek)
            .join(df_notes,
                  how='full',
                  on=['track.artists.name', 'track.name'])
            .join(anti_df,
                  how='anti',
                  on=['track.id'])
            # add bpm
            .join((pl.scan_parquet('processed_data/data_song_bpm.parquet')
                   .with_columns(pl.col(['track.name', 'track.artists.name']).cast(pl.Categorical))
                   ), how='left', on=['track.name', 'track.artists.name'])
            .with_columns(pl.col('bpm').fill_null(pl.col('BPM')))
            # otherwise the None's won't appear in the filter for bpm
            .with_columns(pl.col('bpm').fill_null(0.0),)
            .filter(pl.col('track.artists.name').cast(pl.String).str.contains_any(only_fabulous_people, ascii_case_insensitive=True),
                    pl.col('track.artists.name').cast(pl.String).str.contains_any(
                only_poc_people, ascii_case_insensitive=True),

                # ~pl.col('playlist_name').cast(pl.String).str.contains_any(anti_playlist_input, ascii_case_insensitive=True), #courtesy of Tobias N.
                # pl.unique('playlist_name').over() #has to be diff df such as anti join

                (pl.col('bpm').ge(bpm_slider[0]) & pl.col(
                    'bpm').le(bpm_slider[1])),
                pl.col('country').cast(pl.String).fill_null('').str.contains(
                '|'.join(countries_2_filter)),  # courtesy of Franzi M.
                pl.col('track.name').cast(pl.String).str.contains_any(
                song_input, ascii_case_insensitive=True),
                pl.col('track.artists.name').cast(pl.String).str.contains_any(
                artist_name, ascii_case_insensitive=True),
                pl.col('playlist_name').cast(pl.String).str.contains_any(
                playlist_input, ascii_case_insensitive=True),
                (pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)
                 #   | pl.col('dj_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True) #m3u playlists
                 | pl.col('owner.id').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)),
                pl.col('added_at').dt.to_string().str.contains_any(
                added_2_playlist_date, ascii_case_insensitive=True),  # courtesy of Franzi M.
                pl.col('track.album.release_date').dt.to_string().str.contains_any(
                track_release_date, ascii_case_insensitive=True),  # courtesy of James B.
            )
            .group_by('track.name', 'song_url', 'playlist_count', 'dj_count', )
            .agg(pl.n_unique('playlist_name').alias('matching_playlist_count'),
                 'bpm', 'queer_artist', 'playlist_name', 'track.artists.name',
                 'owner.display_name', 'country', 'poc_artist',
                 # 'apprx_song_position_in_playlist',
                 # 'notes', 'note_source',
                 # connie's notes
                 # 'Starting energy', 'Ending energy', 'BPM', 'Genres', 'Acousticness', 'Difficulty', 'Familiarity', 'Transition type'
                 )
            .with_columns(pl.col('playlist_name').list.unique().list.drop_nulls().list.sort(),
                          pl.col('owner.display_name', 'bpm', 'queer_artist',
                                 # 'apprx_song_position_in_playlist',
                                 'track.artists.name', 'country',
                                 # connie's notes
                                 # 'Starting energy', 'Ending energy', 'BPM', 'Genres', 'Acousticness', 'Difficulty',
                                 # 'Familiarity', 'Transition type'
                                 'poc_artist',
                                 )
                          .list.unique()
                          .list.drop_nulls()
                          .list.sort()
                          .list.head(50),
                          # pl.col('notes', 'note_source').list.unique().list.sort().list.drop_nulls(),
                          hit_terms=pl.col('playlist_name')
                          .cast(pl.List(pl.String))
                          .list.join(', ')
                          .str.to_lowercase()
                              .str.extract_all('|'.join(playlist_input))
                              .list.drop_nulls()
                              .list.unique()
                              .list.sort()
                              .cast(pl.List(pl.Categorical)),
                          )
            .with_columns(pl.col('bpm').list.get(0, null_on_oob=True).fill_null(0).cast(pl.Int32()),
                          # resolves True/False to just True if any True are present
                          pl.col("queer_artist").list.any(),
                          pl.col("poc_artist").list.any(),
                          )
            .select('track.name', 'song_url', 'playlist_count', 'dj_count', 'hit_terms', 'bpm',
                    pl.all().exclude('track.name', 'song_url', 'playlist_count', 'dj_count', 'hit_terms', 'bpm'))
            .sort([pl.col('hit_terms').list.len(),
                   'matching_playlist_count', 'playlist_count', 'dj_count'], descending=True)
            # .pipe(just_a_peek)
            .with_row_index(offset=1)
            .slice(num_results)
        )

        results_df = (song_search_df
                      .with_columns(pl.col('playlist_name').list.head(50))
                      .head(1000).collect(engine="streaming"))
        st.dataframe(results_df,
                     column_config={"song_url": st.column_config.LinkColumn()})

        # playlists_text = ' '.join(song_search_df
        #                         .select(pl.col('playlist_name').cast(pl.List(pl.String)))
        #                         .explode('playlist_name')
        #                         .with_columns(pl.col('playlist_name').str.to_lowercase().str.split(' '))
        #                         .explode('playlist_name')
        #                         .unique()
        #                         .collect(streaming=True)
        #                         ['playlist_name']
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
                .filter(pl.col('bpm').gt(bpm_med) & pl.col('bpm').le(bpm_high))
                .sort('bpm', descending=True)
                .with_row_index('order', offset=1)
                # This gives them the order when combined with the other tracks
                .with_columns((pl.col('order') * 4) - 3,
                              level=pl.lit('high'))
                .head(100)
                # this shuffles that order so the songs aren't strictly high - low bpm
                # .with_columns(pl.col('order').shuffle())
                )

        pl_2 = (results_df
                .filter(pl.col('bpm').gt(bpm_low) & pl.col('bpm').le(bpm_med))
                .sort('bpm', descending=True)
                .with_row_index('order', offset=1)
                .with_columns(pl.col('order') * 2,
                              level=pl.lit('medium'))
                .head(200)
                # .with_columns(pl.col('order').shuffle())
                )

        pl_3 = (results_df
                .filter(pl.col('bpm').le(bpm_low) & pl.col('bpm').gt(0))
                .sort('bpm', descending=True)
                .with_row_index('order', offset=1)
                .with_columns((pl.col('order') * 4) - 1,
                              level=pl.lit('low'))
                .head(100)
                # .with_columns(pl.col('order').shuffle())
                )

        st.dataframe((pl.concat([pl_1, pl_2, pl_3])
                      .select('index', 'level', 'bpm',
                              pl.all().exclude('index', 'bpm', 'level'))
                      .sort('order')
                      .drop('order')
                      ),
                     column_config={"song_url": st.column_config.LinkColumn()})

        # # 1 2 3 2 1 2 3 2 1

        # attempt at better playlist generation
        # # Tag levels based on BPM
        # results_df2 = (results_df
        #                .with_columns(level = pl.when(pl.col("bpm") > bpm_med)
        #                                         .then(pl.lit("high"))
        #                                         .when(pl.col("bpm") > bpm_low)
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
        #                 prev_bpm = h1["bpm"][i]

        #                 # Step 2: Medium song
        #                 m1 = sample_with_bpm_range(medium_df, prev_bpm)
        #                 playlist_parts.append(m1)
        #                 prev_bpm = m1["bpm"][i]

        #                 # Step 3: Low song
        #                 l1 = sample_with_bpm_range(low_df, prev_bpm)
        #                 playlist_parts.append(l1)
        #                 prev_bpm = l1["bpm"][i]

        #                 # Step 4: Medium song
        #                 m2 = sample_with_bpm_range(medium_df, prev_bpm)
        #                 playlist_parts.append(m2)
        #                 prev_bpm = m2["bpm"][i]
        #         except:
        #                 pass

        # # Combine and add index
        # playlist_df = pl.concat(playlist_parts).with_row_index(name="order", offset=1)

        # # Display
        # st.dataframe((playlist_df
        #               .select('index', 'level', 'bpm',
        #                       pl.all().exclude('index', 'bpm', 'level'))
        #               .drop('order')
        #               ),
        # column_config={"song_url": st.column_config.LinkColumn("Song")}
        # )

    st.markdown(f"#### ")


# courtesy of Vishal S
playlist_locator_toggle = st.toggle("Find a Playlist 💿")
if playlist_locator_toggle:
    playlist_col1, playlist_col2 = st.columns(2)
    with playlist_col1:
        song_input = st.text_input("Contains the song:").lower().split(',')
        playlist_input = st.text_input("Playlist name:").lower().split(',')
    with playlist_col2:
        dj_input = st.text_input("DJ name:").lower().split(',')
        anti_playlist_input2 = st.text_input(
            "Not in playlist name: ").lower().split(',')

    if anti_playlist_input2 == ['']:
        anti_playlist_input2 = [
            'this_is_a_bogus_value_to_hopefully_not_break_things']

    # if any(val for val in [playlist_input, song_input, dj_input]):
    if st.button("Search playlists", type="primary"):

        log_query("Search playlists", {'song_input': song_input,
                                       'song_input': song_input,
                                       'dj_input': dj_input,
                                       'playlist_input': playlist_input,
                                       'anti_playlist_input': anti_playlist_input2,
                                       }
                  )

        st.dataframe(df
                     .filter(~pl.col('playlist_name').cast(pl.String).str.contains_any(anti_playlist_input2, ascii_case_insensitive=True),
                             pl.col('playlist_name').cast(pl.String).str.contains_any(
                         playlist_input, ascii_case_insensitive=True),
                         pl.col('track.name').cast(pl.String).str.contains_any(
                         song_input, ascii_case_insensitive=True),

                         (pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)
                          # | pl.col('dj_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True) #m3u playlists
                          | pl.col('owner.id').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)),

                         # pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True),
                     )
                     .group_by('playlist_name', 'playlist_url')
                     .agg('owner.display_name', pl.n_unique('track.name').alias('song_count'),
                          pl.n_unique('track.artists.name').alias('artist_count'), 'track.name')
                     .with_columns(pl.col('owner.display_name', 'track.name').list.unique().list.sort(),)
                     .head(500).collect(streaming=True),
                     column_config={
                         "playlist_url": st.column_config.LinkColumn()}
                     )
    st.markdown(f"#### ")


@st.cache_data
def djs_data():
    return (df
            .group_by('owner.display_name', 'owner_url')
            .agg(pl.n_unique('track.name').alias('song_count'),
                 pl.n_unique('track.artists.name').alias('artist_count'),
                 pl.n_unique('playlist_name').alias('playlist_count'),
                 'playlist_name',
                 )
            .with_columns(pl.col('playlist_name')
                          .list.unique()
                          .list.drop_nulls()
                          .list.sort()
                          .list.head(50)
                          )
            .sort(pl.col('playlist_count'), descending=True)
            .head(2000)
            .collect(streaming=True)
            )


djs_data = djs_data()


# courtesy of Lino V
search_dj_toggle = st.toggle("DJ insights 🎧")

if search_dj_toggle:
    dj_col1, dj_col2 = st.columns(2)
    with dj_col1:
        dj_input = st.text_input(
            "DJ name/ID (ex. Kasia Stepek or 1185428002)").lower().split(',')
        # dj_id = id_input.lower().split(',')
    with dj_col2:
        dj_playlist_input = st.text_input(
            "DJ playlist name:").lower().split(',')

    if (dj_input == ['']) and (dj_playlist_input == ['']):
        st.dataframe(djs_data,
                     column_config={"owner_url": st.column_config.LinkColumn()})

    # else:
    if st.button("Search djs", type="primary"):

        log_query("Search djs", {'dj_input': dj_input,
                                 'dj_playlist_input': dj_playlist_input,
                                 })

        dj_search_df = (df
                        .filter((pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)
                                 # | pl.col('dj_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True) #m3u playlists
                                 | pl.col('owner.id').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True))
                                & pl.col('playlist_name').cast(pl.String).str.contains_any(dj_playlist_input, ascii_case_insensitive=True),
                                )
                        .group_by('owner.display_name', 'owner_url')
                        .agg(pl.n_unique('track.name').alias('song_count'),
                             pl.n_unique('track.artists.name').alias(
                                 'artist_count'),
                             pl.n_unique('playlist_name').alias(
                                 'playlist_count'),
                             'playlist_name',
                             )
                        .with_columns(pl.col('playlist_name')
                                      .list.eval(pl.when(pl.element()
                                                 .cast(pl.String)
                                                 .str.contains_any(dj_playlist_input, ascii_case_insensitive=True))
                                                 .then(pl.element()))
                                      .list.unique()
                                      .list.drop_nulls()
                                      .list.sort()
                                      .list.head(50)
                                      )
                        .sort(pl.col('playlist_count'), descending=True)
                        .head(100)
                        .collect(streaming=True)
                        )
        st.dataframe(dj_search_df,
                     column_config={"owner_url": st.column_config.LinkColumn()})

        total_djs_from_search = dj_search_df.select(pl.n_unique('owner.display_name'))[
            'owner.display_name'][0]
    # elif dj_id:
        if total_djs_from_search > 0 and total_djs_from_search <= 10:  # so it doesn't have to process if nothing

            djs_music = (df
                         .filter((pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)
                                  # | pl.col('dj_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True) #m3u playlists
                                  | pl.col('owner.id').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True))
                                 )
                         .select('track.name', 'owner.display_name', 'dj_count', 'playlist_count', 'playlist_name', 'song_url')
                         .unique()
                         )
            # too much data now that we have more music, that list is blowing up the streamlit
            others_music = (df
                            .filter(~(pl.col('owner.display_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True)
                                      # | pl.col('dj_name').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True) #m3u playlists
                                      | pl.col('owner.id').cast(pl.String).str.contains_any(dj_input, ascii_case_insensitive=True))
                                    )
                            .select('track.name', 'owner.display_name', 'dj_count', 'playlist_count', 'song_url')
                            )

            # st.text(f"Music unique to _{', '.join(dj_input)}_")
            # st.dataframe(djs_music.join(others_music,
            #                         how='anti',
            #                         on=['track.name', pl.col('owner.display_name').cast(pl.String),
            #                                 'dj_count', 'playlist_count', 'song_url'])
            #         .group_by(pl.all().exclude('playlist_name'))
            #         .agg('playlist_name')
            #         .sort('playlist_count', descending=True)
            #         .filter(pl.col('dj_count').eq(1))
            #         .head(100)
            #         .collect(streaming=True),
            #         column_config={"song_url": st.column_config.LinkColumn()})

            # st.text(f"Popular music _{', '.join(dj_input)}_ doesn't play")
            # st.dataframe(others_music.join(djs_music, how='anti',
            #                 on=['track.name', 'dj_count',
            #                 'playlist_count', 'song_url'])
            #         .group_by(pl.all().exclude('owner.display_name'))
            #         .agg('owner.display_name')
            #         .with_columns(pl.col('owner.display_name').list.head(50))
            #         .sort('dj_count', 'playlist_count', descending=True)
            #         .head(200)
            #         .collect(streaming=True),
            #         column_config={"song_url": st.column_config.LinkColumn()})

    st.markdown(f"#### Compare DJs:")
    # dj_list = sorted(df
    #                  .select('owner.display_name')
    #                  .cast(pl.String)
    #                  .unique()
    #                  .drop_nulls()
    #                  .collect(streaming=True)
    #                  ['owner.display_name']
    #                  .to_list()
    #                  )

    # st.dataframe(df
    #                 .group_by('owner.display_name')
    #                 .agg(song_count = pl.n_unique('track.name'),
    #                         playlist_count = pl.n_unique('playlist_name'),
    #                         dj_count = pl.n_unique('owner.display_name'),
    #                         )
    #                 .sort('owner.display_name')
    #                 .collect(streaming=True)
    #         )

    # djs_selectbox = st.multiselect("Compare these DJ's music:", dj_list)
    compare_1, compare_2 = st.columns(2)
    with compare_1:
        dj_compare_1 = st.text_input("DJ/user 1 to compare:").lower()
    with compare_2:
        dj_compare_2 = st.text_input("DJ/user 2 to compare:").lower()

    if st.button("Compare DJs/users", type="primary"):

        log_query("Search djs", {'dj_compare_1': dj_compare_1,
                                 'dj_compare_2': dj_compare_2,
                                 })

        st.dataframe(df
                     .filter(pl.col('owner.display_name').cast(pl.String).str.to_lowercase().eq(dj_compare_1)
                             | pl.col('owner.id').cast(pl.String).str.to_lowercase().eq(dj_compare_1)
                             | pl.col('owner.display_name').cast(pl.String).str.to_lowercase().eq(dj_compare_2)
                             | pl.col('owner.id').cast(pl.String).str.to_lowercase().eq(dj_compare_2)
                             )
                     .group_by('owner.display_name')
                     .agg(song_count=pl.n_unique('track.name'),
                          playlist_count=pl.n_unique('playlist_name'),
                          )
                     .sort('owner.display_name')
                     .collect(streaming=True)
                     )

        dj_1_df = (df
                   .filter(pl.col('owner.display_name').cast(pl.String).str.to_lowercase().eq(dj_compare_1)
                           | pl.col('owner.id').cast(pl.String).str.to_lowercase().eq(dj_compare_1))
                   .select('track.name', 'song_url', 'dj_count', 'playlist_count')
                   )
        dj_2_df = (df
                   .filter(pl.col('owner.display_name').cast(pl.String).str.to_lowercase().eq(dj_compare_2)
                           | pl.col('owner.id').cast(pl.String).str.to_lowercase().eq(dj_compare_2))
                   .select('track.name', 'song_url', 'dj_count', 'playlist_count')
                   )

        st.text(f"Music {dj_compare_1} has, but {dj_compare_2} doesn't.")
        st.dataframe(dj_1_df
                     .join(dj_2_df,
                           how='anti',
                           on=['track.name', 'song_url']
                           )
                     .unique()
                     .sort('dj_count', descending=True)
                     .head(500).collect(streaming=True),
                     column_config={"song_url": st.column_config.LinkColumn()})

    st.markdown(f"#### ")


@st.cache_data
def region_data():
    return (df
            .group_by('region')
            .agg(song_count=pl.n_unique('track.name'),
                 playlist_count=pl.n_unique('playlist_name'),
                 dj_count=pl.n_unique('owner.display_name'),
                 djs=pl.col('owner.display_name'),
                 )
            .with_columns(pl.col('djs').list.unique().list.head(50))
            .sort('region')
            .collect(streaming=True)
            )


@st.cache_data
def country_data():
    return (df
            .group_by('country')
            .agg(song_count=pl.n_unique('track.name'),
                 playlist_count=pl.n_unique('playlist_name'),
                 dj_count=pl.n_unique('owner.display_name'),
                 djs=pl.col('owner.display_name'),
                 )
            .with_columns(pl.col('djs').list.unique().list.head(50))
            .sort('country')
            .collect(streaming=True)
            )


# courtesy of Lino V
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
        st.markdown(
            f"#### What are the most popular songs only played in {region_selectbox}?")

        region_df = (pl.scan_parquet('processed_data/data_unique_per_region.parquet')
                     #  .pipe(wcs_specific)
                     .filter(pl.col('region').cast(pl.String) == region_selectbox,
                             # pl.col('geographic_region_count').eq(1)
                             )
                     # .group_by('track.name', 'song_url', 'dj_count', 'playlist_count', 'region', 'geographic_region_count')
                     # .agg(pl.col('owner.display_name').unique())
                     # .with_columns(pl.col('owner.display_name').list.unique())
                     # .unique()
                     .sort('playlist_count', 'dj_count', descending=True)
                     )

        st.dataframe(region_df.head(1000).collect(streaming=True),
                     column_config={"song_url": st.column_config.LinkColumn()})

    st.markdown(f"#### Comparing Countries' music:")
    countries_selectbox = st.multiselect(
        "Compare these countries' music:", countries)
    # country_1, country_2 = st.columns(2)
    # with country_1:
    #         country_compare_1 = st.text_input("Country 1:").lower()
    # with country_2:
    #         country_compare_2 = st.text_input("Country 2:").lower()

    if st.button("Compare countries", type="primary"):

        log_query("Comparing Countries' music", {'countries_selectbox': countries_selectbox,
                                                 })

        countries_df = df.filter(pl.col('country').cast(pl.String).str.contains_any(countries_selectbox),
                                 pl.col('dj_count').gt(3),
                                 pl.col('playlist_count').gt(3))

        country_1_df = (countries_df
                        .filter(pl.col('country').cast(pl.String).eq(countries_selectbox[0]))
                        .select('track.name', 'song_url', 'dj_count', 'playlist_count')
                        )

        country_2_df = (countries_df
                        .filter(pl.col('country').cast(pl.String).eq(countries_selectbox[1]))
                        .select('track.name', 'song_url', 'dj_count', 'playlist_count')
                        )

        # st.dataframe(country_1_df._fetch(10000))
        st.text(
            f"{countries_selectbox[0]} music not in {countries_selectbox[1]}")
        st.dataframe(country_1_df.join(country_2_df,
                                       how='anti',
                                       on=['track.name', 'song_url']
                                       )
                     .unique()
                     .sort('dj_count', descending=True)
                     .head(300).collect(streaming=True),
                     # ._fetch(10000),
                     column_config={"song_url": st.column_config.LinkColumn()})
        st.markdown(f"#### ")


# courtesy of Vincent M
songs_together_toggle = st.toggle("Songs most played together")

if songs_together_toggle:

    song_combo_col1, song_combo_col2 = st.columns(2)
    with song_combo_col1:
        song_input = st.text_input("Song Name/ID:")
    with song_combo_col2:
        artist_name_input = st.text_input("Song artist name:").lower()

    song_input_prepped = song_input.lower()

    # st.dataframe(df
    #         .select('song_number', 'track.name', 'playlist_name', 'track.id', 'playlist_url', 'owner.display_name', 'track.artists.id'
    #                 )
    #         .unique()
    #         .sort('playlist_url', 'song_number')

    #         .with_columns(pair1 = pl.when(pl.col('song_number').shift(-1) > pl.col('song_number'))
    #                                 .then(pl.concat_str(pl.col('track.name'), pl.lit(': '), pl.col('track.id'), pl.lit(' --- '),
    #                                                         pl.col('track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(-1),
    #                                                         )),
    #                         pair2 = pl.when(pl.col('song_number').shift(1) < pl.col('song_number'))
    #                                 .then(pl.concat_str(pl.col('track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(1), pl.lit(' --- '),
    #                                                         pl.col('track.name'), pl.lit(': '), pl.col('track.id'),
    #                                                         )),
    #                         )
    #         .with_columns(pair = pl.concat_list('pair1', 'pair2'))
    #         .explode('pair')
    #         .select('pair', 'playlist_name', 'owner.display_name',
    #                 )
    #         .drop_nulls()
    #         .unique()
    #         .with_columns(pl.col('pair').str.split(' --- ').list.sort().list.join(' --- '))
    #         .group_by('pair')
    #         .agg(pl.n_unique('playlist_name').alias('times_played_together'), 'playlist_name', 'owner.display_name',
    #                 )
    #         .with_columns(pl.col('playlist_name').list.unique(),
    #                         pl.col('owner.display_name').list.unique())
    #         .filter(~pl.col('playlist_name').list.join(', ').str.contains_any(['The Maine', 'delete', 'SPOTIFY']),
    #                 pl.col('times_played_together').gt(1),
    #                 )
    #         .filter(pl.col('pair').str.to_lowercase().str.contains(song_input_prepped),
    #                 pl.col('track.artists.id').list.join(', ').str.to_lowercase().str.contains(artist_name_input)
    #                 )
    #         .with_columns(pl.col('pair').str.split(' --- '))
    #         .sort('times_played_together',
    #                 pl.col('owner.display_name').list.len(),
    #                 descending=True)
    #         .head(100).collect(streaming=True),
    #          column_config={"playlist_url": st.column_config.LinkColumn()}
    #         )

    if st.button("Search songs played together", type="primary"):
        # if (song_input_prepped + artist_name_input).strip() != '':
        st.markdown(f"#### Most common songs to play after _{song_input}_:")
        st.dataframe(df
                     .filter(pl.col('actual_social_set') == True,
                             )
                     .select('song_number', 'track.name', 'playlist_name', 'track.id', 'song_url',
                             'owner.display_name', 'track.artists.name',
                             )
                     .unique()
                     .sort('playlist_name', 'song_number')
                     .with_columns(pair1=pl.when(pl.col('song_number').shift(-1) > pl.col('song_number'))
                                   .then(pl.concat_str(pl.col('track.name'), pl.lit(': '), pl.col('track.id'), pl.lit(' --- '),
                                                       pl.col(
                                       'track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(-1),
                                   )),
                                   pair2=pl.when(pl.col('song_number').shift(
                                       1) < pl.col('song_number'))
                                   .then(pl.concat_str(pl.col('track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(1), pl.lit(' --- '),
                                                       pl.col('track.name'), pl.lit(
                                       ': '), pl.col('track.id'),
                                   )),
                                   )
                     .with_columns(pair=pl.concat_list('pair1', 'pair2'))
                     .explode('pair')
                     .select('pair', 'playlist_name', 'owner.display_name', 'track.artists.name', 'track.name', 'song_url',
                             )
                     .drop_nulls()
                     .unique()
                     .with_columns(pl.col('pair').str.split(' --- ').list.sort().list.join(' --- '))
                     .group_by('pair')
                     .agg(pl.n_unique('playlist_name').cast(pl.UInt8).alias('times_played_together'),
                          'playlist_name', 'owner.display_name', 'track.artists.name', 'track.name', 'song_url')
                     .with_columns(pl.col('playlist_name').list.unique(),
                                   pl.col('owner.display_name').list.unique())
                     .filter(~pl.col('playlist_name').cast(pl.List(pl.String)).list.join(', ').str.contains_any(['The Maine', 'delete', 'SPOTIFY']),
                             pl.col('times_played_together').gt(1),
                             )
                     .filter(pl.col('pair').str.split(' --- ').list.get(0, null_on_oob=True).str.to_lowercase().str.contains(song_input_prepped),
                             pl.col('track.artists.name').cast(pl.List(pl.String)).list.join(', ').str.to_lowercase().str.contains(artist_name_input))
                     .with_columns(pl.col('pair').str.split(' --- '))
                     .sort('times_played_together',
                           pl.col('owner.display_name').list.len(),
                           descending=True)
                     .head(100).collect(streaming=True),
                     column_config={"song_url": st.column_config.LinkColumn()}
                     )

        st.markdown(f"#### Most common songs to play before _{song_input}_:")

        st.dataframe(df
                     .filter(pl.col('actual_social_set') == True)
                     .select('song_number', 'track.name', 'playlist_name', 'track.id', 'song_url', 'owner.display_name', 'track.artists.name')
                     .unique()
                     .sort('playlist_name', 'song_number')
                     .with_columns(pair1=pl.when(pl.col('song_number').shift(-1) > pl.col('song_number'))
                                   .then(pl.concat_str(pl.col('track.name'), pl.lit(': '), pl.col('track.id'), pl.lit(' --- '),
                                                       pl.col(
                                       'track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(-1),
                                   )),
                                   pair2=pl.when(pl.col('song_number').shift(
                                       1) < pl.col('song_number'))
                                   .then(pl.concat_str(pl.col('track.name').shift(-1), pl.lit(': '), pl.col('track.id').shift(1), pl.lit(' --- '),
                                                       pl.col('track.name'), pl.lit(
                                       ': '), pl.col('track.id'),
                                   )),
                                   )
                     .with_columns(pair=pl.concat_list('pair1', 'pair2'))
                     .explode('pair')
                     .select('pair', 'playlist_name', 'owner.display_name', 'track.artists.name', 'track.name', 'song_url',
                             )
                     .drop_nulls()
                     .unique()
                     .with_columns(pl.col('pair').str.split(' --- ').list.sort().list.join(' --- '))
                     .group_by('pair')
                     .agg(pl.n_unique('playlist_name').cast(pl.UInt8).alias('times_played_together'),
                          'playlist_name', 'owner.display_name', 'track.artists.name', 'track.name', 'song_url')
                     .with_columns(pl.col('playlist_name').list.unique(),
                                   pl.col('owner.display_name').list.unique())
                     .filter(~pl.col('playlist_name').cast(pl.List(pl.String)).list.join(', ').str.contains_any(['The Maine', 'delete', 'SPOTIFY']),
                             pl.col('times_played_together').gt(1),
                             )
                     .filter(pl.col('pair').str.split(' --- ').list.get(1, null_on_oob=True).str.to_lowercase().str.contains(song_input_prepped),
                             pl.col('track.artists.name').cast(pl.List(pl.String)).list.join(', ').str.to_lowercase().str.contains(artist_name_input))
                     .with_columns(pl.col('pair').str.split(' --- '))
                     .sort('times_played_together',
                           pl.col('owner.display_name').list.len(),
                           descending=True)
                     .head(100).collect(streaming=True),
                     column_config={"song_url": st.column_config.LinkColumn()}
                     )
    st.link_button("Andreas' connected-songs visualization!",
                   'https://loewclan.de/song-galaxy/')
    st.markdown(f"#### ")


lyrics_toggle = st.toggle("Search lyrics 📋")
if lyrics_toggle:

    st.write(
        f"from {df_lyrics.select('artist', 'song').unique().collect(streaming=True).shape[0]:,} songs")
    lyrics_col1, lyrics_col2 = st.columns(2)
    with lyrics_col1:
        song_input = st.text_input("Song:")
        lyrics_input = st.text_input("In lyrics:").lower().split(',')

    with lyrics_col2:
        artist_input = st.text_input("Artist:")
        anti_lyrics_input = st.text_input("Not in lyrics:").lower().split(',')

    if anti_lyrics_input == ['']:
        anti_lyrics_input = [
            'this_is_a_bogus_value_to_hopefully_not_break_things']

    if st.button("Search lyrics", type="primary"):
        st.dataframe(
            df_lyrics.with_columns(
                pl.col(['song', 'artist']).cast(pl.Categorical))
            .join(df.select('song_url', 'playlist_count', 'dj_count',
                            song=pl.col('track.name'),
                            artist=pl.col('track.artists.name')).unique(),
                  how='left', on=['song', 'artist'])
            .filter(~pl.col('lyrics').str.contains_any(anti_lyrics_input, ascii_case_insensitive=True),
                    pl.col('lyrics').str.contains_any(
                    lyrics_input, ascii_case_insensitive=True),
                    pl.col('song').cast(pl.String).str.contains_any(
                    [song_input], ascii_case_insensitive=True),
                    pl.col('artist').cast(pl.String).str.contains_any(
                    [artist_input], ascii_case_insensitive=True),
                    )
            .with_columns(matched_lyrics=pl.col('lyrics')
                          .str.to_lowercase()
                          .str.extract_all('|'.join(lyrics_input))
                          .list.eval(pl.element().str.to_lowercase())
                          .list.unique(),
                          )

            # otherwise there will be multiple rows for each song variation
            .group_by(pl.all().exclude('song_url', 'playlist_count', 'dj_count',))
            .agg('song_url', 'playlist_count', 'dj_count',)
            .with_columns(pl.col('song_url').list.get(0),  # otherwise multiple urls will be smashed together
                          playlists=pl.col('playlist_count').list.sort(
                              descending=True).list.get(0),
                          djs=pl.col('dj_count').list.sort(
                              descending=True).list.get(0),
                          )
            .drop('playlist_count', 'dj_count',)
            .unique()
            .sort(pl.col('matched_lyrics').list.len(), 'playlists', 'djs', descending=True, nulls_last=True)
            .head(100)
            .collect(streaming=True),
            column_config={"song_url": st.column_config.LinkColumn()}
        )


st.markdown("# ")
st.markdown("# ")
st.markdown("#### WCS resourses/apps by others:")
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
* What music was played at Budafest, but NOT at Westie Spring Thing (Courtesy of Nicole!)
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

#### It broke ☹️
* The back-end I'm using is free, but I would upgrade to a new system once there are enough users willing to help pay for it.
* Yes, we're doing some expensive processing on 600MB+ data with a machine of 1GB memory 😬 (You usually need 5x-10x more memory in order to open a file of a particular size… never mind do anything with it. I'm using lots of clever memory tricks so it can just baaaaarely squeeze inside the memory limits, but if multiple people hit it... ☠️
* It requires a manual reboot - so if you're working on something critical, ping me so I can restart it (whatsapp/fb)

#### Errors:
* Please report any errors you notice, or anything that doesn't make sense and I'll try to get to it!

#### Things to consider:
* Since the majority of data is based on user adding songs to their own playlists, user-generated vs DJ-generated, the playlists may not reflect actual played sets (except when specified). The benefit, while I work on rounding up DJs not on Spotify, is that we get to see the ground truth of what users actually enjoy (such as songs missed by the GSDJ Top 10 lists).
""")
