"""Methods for pre-processing the data into more efficient formats at build time."""
from typing import Callable, Final, Literal
import math
import os

import polars as pl

from utils.additional_data import actual_wcs_djs, queer_artists, poc_artists
from utils.common.temp_files import TempFileTracker, with_temp_files
from utils.playlist_classifiers import extract_dates_from_name, extract_tags_from_name
from utils.search import (
    COUNTRY_DATA_FILE,
    DATA_DIR,
    PLAYLIST_DATA_FILE,
    PLAYLIST_ORIGINAL_DATA_FILE,
    PLAYLIST_TRACKS_DATA_FILE,
    PLAYLIST_TRACKS_ORIGINAL_DATA_FILE,
    REGION_DATA_FILE,
    TAGS_DATA_FILE,
    TEMP_DATA_DIR,
    TRACK_ADJACENT_DATA_FILE,
    TRACK_CANONICAL_DATA_FILE,
    TRACK_DATA_FILE,
    TRACK_DUPLICATES_DATA_FILE,
    TRACK_LYRICS_DATA_FILE,
    TRACK_ORIGINAL_DATA_FILE,
    TRACK_PLAYLISTS_DATA_FILE,
    TRACK_TAGS_DATA_FILE,
    UNPROCESSED_PLAYLISTS_DATA_FILE,
    UNPROCESSED_TRACK_BPM_DATA_FILE,
    UNPROCESSED_TRACK_LYRICS_DATA_FILE,
)
from utils.tables import Playlist, PlaylistOwner, PlaylistTags, PlaylistTrack, Stats, Tag, Track, TrackAdjacent, TrackLyrics, TrackTag, TrackTags

# Temporary files are also stored in processed_data/
TEMP_DATA_DIR: Final = DATA_DIR

# NOTE: Setting TRACK_ID_DTYPE and PLAYLIST_ID_DTYPE to pl.Categorical
#       instead of pl.String blows up the size of data_playlist_songs.parquet
#       by a factor of more than 4x (227 MB vs. 41 MB), so it seems
#       like pl.String is the only right answer here.

TRACK_ID_DTYPE = pl.String
TRACK_BPM_DTYPE = pl.UInt8
TRACK_NAME_DTYPE = pl.String
TRACK_ARTIST_DTYPE = pl.String
PLAYLIST_ID_DTYPE = pl.String
OWNER_ID_DTYPE = pl.String
OWNER_NAME_DTYPE = pl.String


# Simplistic file tracker to verify that operations
# are invoked in the correct order.
opened_files: set[str] = set()
written_files: set[str] = set()


def reset_file_tracker():
    opened_files.clear()
    written_files.clear()


def write_to_file(data: pl.LazyFrame | pl.DataFrame, file_name: str, format: Literal['parquet', 'csv']):
    print(f'Writing {file_name}...')

    if file_name in opened_files:
        print(f'WARNING: {file_name} has already been read during this session.'
              + ' This should not have happened, and likely indicates an implementation error.')

    if file_name in written_files:
        print(f'WARNING: {file_name} has already been written to during this session.'
              + ' This should not have happened, and likely indicates an implementation error.')

    written_files.add(file_name)

    print(f'- SCHEMA: {data.collect_schema()}')

    if isinstance(data, pl.DataFrame):
        getattr(data, 'write_' + format)(file_name)
    else:
        getattr(data, 'sink_' + format)(file_name)

    file_size = os.path.getsize(file_name)
    print(f'- SIZE: {file_size:,} bytes')
    print('')


def write_to_parquet_file(data: pl.LazyFrame | pl.DataFrame, file_name: str):
    write_to_file(data, file_name, 'parquet')


def write_to_csv_file(data: pl.LazyFrame | pl.DataFrame, file_name: str):
    write_to_file(data, file_name, 'csv')


def scan_file(file_name: str, format: Literal['parquet', 'csv']) -> pl.LazyFrame:
    if file_name in written_files:
        print(f'<< Reading {file_name} from previous step...')
    else:
        print(f'<< Reading {file_name}...')

    return pl.scan_parquet(file_name)


def scan_parquet_file(file_name: str) -> pl.LazyFrame:
    return scan_file(file_name, 'parquet')


def scan_csv_file(file_name: str) -> pl.LazyFrame:
    return scan_file(file_name, 'csv')

############################
# Batch processing helpers #
############################


def create_temp_dir() -> str:
    temp_dir: Final = TEMP_DATA_DIR
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    return temp_dir


@with_temp_files()
def process_in_batches(
    batch_temp_files: TempFileTracker,
    data: pl.LazyFrame,
    map: Callable[[pl.LazyFrame], pl.LazyFrame],
    *,
    batch_by: str,
    batch_values: pl.LazyFrame,
    batch_name: str,
    batch_size: int,
    output_name: str,
    sort_by: str = '',
):
    if not sort_by:
        sort_by = batch_by

    if batch_by or batch_values is not None:
        row_count = batch_values.select(pl.len()).collect().item()
    else:
        row_count = data.select(pl.len()).collect().item()

    batch_count = int(math.ceil(row_count / batch_size))
    merged: pl.LazyFrame | None = None

    print(f"Processing {row_count:,} {batch_name} in {batch_count:,} batches of {batch_size:,} items...")

    for batch_index in range(0, batch_count):
        batch_start = batch_index * batch_size

        print(f"Processing batch {batch_index:,}/{batch_count:,}")

        if batch_by or batch_values is not None:
            batch_input = data\
                .join(batch_values.slice(batch_start, batch_size),
                      how='semi', on=batch_by)
        else:
            batch_input = data\
                .slice(batch_start, batch_size)

        batch_output = batch_input\
            .pipe(map)\
            .sort(sort_by)

        # Write batch result to temp file
        temp_file = batch_temp_files.register_for_deletion(
            create_temp_dir() + f'temp_{batch_name}_batch_{batch_index}.parquet')
        write_to_parquet_file(batch_output, temp_file)

        # Add temp file to final merge
        batch_output = scan_parquet_file(temp_file)
        merged = (batch_output if merged is None else
                  merged.merge_sorted(batch_output, sort_by))

    print("Merging batches...")

    write_to_parquet_file(merged, output_name)


##############################
# Actual pre-processing code #
##############################

def process_country_data():
    source_data = scan_parquet_file(UNPROCESSED_PLAYLISTS_DATA_FILE)

    locations = (
        source_data.select('location')
        .drop_nulls()
        .filter(pl.col('location').ne(''))
        .unique()
        .select(
            pl.col('location'),
            pl.col('location').str.split(' - ').list.get(
                0, null_on_oob=True).cast(pl.String).alias('region'),
            pl.col('location').str.split(' - ').list.get(
                1, null_on_oob=True).cast(pl.String).alias('country'))
        .sort('location')
        .collect(engine='streaming'))

    countries = (
        locations.lazy()
                 .select(pl.col('country').cast(pl.String))
                 .filter(pl.col('country').ne(''))
                 .drop_nulls()
                 .unique()
                 .sort('country')
                 .collect(engine='streaming'))

    regions = (
        locations.lazy()
                 .select(pl.col('region').cast(pl.String))
                 .filter(pl.col('region').ne(''))
                 .drop_nulls()
                 .unique()
                 .sort('region')
                 .collect(engine='streaming'))

    # Write pre-processed data to csv files
    write_to_csv_file(regions, REGION_DATA_FILE)
    write_to_csv_file(countries, COUNTRY_DATA_FILE)


def get_region_enum() -> pl.Enum:
    return pl.Enum(pl.read_csv(REGION_DATA_FILE)['region'])


def get_country_enum() -> pl.Enum:
    return pl.Enum(pl.read_csv(COUNTRY_DATA_FILE)['country'])


def process_playlist_and_song_data(*, prepare_deduplication: bool = False):
    source_data = scan_parquet_file(UNPROCESSED_PLAYLISTS_DATA_FILE)
    bpm_data = scan_parquet_file(UNPROCESSED_TRACK_BPM_DATA_FILE)

    LOCATION: Final = 'location'
    REGION: Final = 'region'
    COUNTRY: Final = 'country'

    def null_if_empty(expr: pl.Expr) -> pl.Expr:
        return pl.when(expr.ne('')).then(expr).otherwise(pl.lit(None))

    print(source_data.schema)
    # Rename columns for consistency
    typed_source_data = source_data.rename({
        'playlist_id': Playlist.id,
        'name': Playlist.name,
        'owner.display_name': PlaylistOwner.name,
        'song_number': PlaylistTrack.number,
        'added_at': PlaylistTrack.added_at,

    }).select(
        # Playlist columns
        pl.col('playlist.id').pipe(null_if_empty).cast(PLAYLIST_ID_DTYPE),
        pl.col(Playlist.name),
        pl.col(PlaylistOwner.id).pipe(null_if_empty).cast(OWNER_ID_DTYPE),
        pl.col(PlaylistOwner.name).pipe(null_if_empty).cast(OWNER_NAME_DTYPE),

        # Track columns
        pl.col(Track.id).cast(TRACK_ID_DTYPE),
        pl.col(Track.name).cast(TRACK_NAME_DTYPE),
        pl.col(Track.artist_names).cast(TRACK_ARTIST_DTYPE),
        pl.col(Track.release_date).cast(pl.Date),

        # PlaylistTrack columns
        pl.col(PlaylistTrack.number).cast(pl.UInt16),
        pl.col(PlaylistTrack.added_at).cast(pl.Date),

        # Location columns
        pl.col(LOCATION).pipe(null_if_empty),
        pl.col(LOCATION).str.split(' - ')
          .list.get(0, null_on_oob=True)
          .pipe(null_if_empty).cast(get_region_enum())
          .alias(REGION),
        pl.col(LOCATION).str.split(' - ')
          .list.get(1, null_on_oob=True)
          .pipe(null_if_empty).cast(get_country_enum())
          .alias(COUNTRY),
    )

    #############
    # PLAYLISTS #
    #############

    # Extract unique playlists
    playlists = typed_source_data\
        .select(Playlist.id,
                Playlist.name,
                PlaylistOwner.id,
                PlaylistOwner.name,
                pl.col(COUNTRY).alias(Playlist.country),
                pl.col(REGION).alias(Playlist.region))\
        .unique(Playlist.id)

    # Detect social playlists
    _is_social_set = (
        pl.col(Playlist.extracted_dates).list.len().gt(0)
        | pl.col(Playlist.name).str.contains_any(['social', 'party', 'soir'], ascii_case_insensitive=True)
    )

    _is_wcs_dj = (
        pl.col(PlaylistOwner.id).cast(pl.String).str.contains_any(
            actual_wcs_djs, ascii_case_insensitive=True)
        | pl.col(PlaylistOwner.name).cast(pl.String).eq('Connie Wang')
        | pl.col(PlaylistOwner.name).cast(pl.String).eq('Koichi Tsunoda')
    )

    playlists = playlists.with_columns(
        extract_dates_from_name(pl.col(Playlist.name), sort=True).cast(
            pl.List(pl.String)).alias(Playlist.extracted_dates),
    )

    # Add stub columns for track deduplication
    playlists = playlists.with_columns(
        pl.lit(None).cast(pl.UInt32).alias(Stats.song_count),  # Stub, will be be calculated after deduplication
        pl.lit(None).cast(pl.UInt32).alias(Stats.artist_count),  # Stub, will be calculated after deduplication
    )

    playlists = playlists.with_columns(
        _is_social_set.alias(Playlist.is_social_set),
        _is_wcs_dj.alias(PlaylistOwner.is_wcs_dj),
    )

    # Final step: Sort by ID to speed up lookup from the Parquet file
    playlists = playlists\
        .sort(Playlist.id)

    # Write pre-processed data to parquet file
    create_temp_dir()
    write_to_parquet_file(
        playlists,
        PLAYLIST_ORIGINAL_DATA_FILE if prepare_deduplication else PLAYLIST_DATA_FILE)

    ##########
    # TRACKS #
    ##########

    temp_file = create_temp_dir() + "temp_track_ids.parquet"
    track_ids = typed_source_data\
        .select(Track.id)\
        .unique(Track.id)\
        .sort(Track.id)
    write_to_parquet_file(track_ids, temp_file)
    track_ids = scan_parquet_file(temp_file)

    def process_track_batch(source_data_batch: pl.LazyFrame) -> pl.LazyFrame:
        return source_data_batch.select(
            Track.id,
            Track.name,
            Track.artist_names,
            Track.release_date,
            pl.col(REGION).alias(Track.region),
            pl.col(COUNTRY).alias(Track.country),
            Playlist.id,
            PlaylistOwner.name,
        ).group_by(Track.id).agg(
            pl.col(Track.name).drop_nulls().first(),
            pl.col(Track.artist_names).drop_nulls()
            .unique(maintain_order=True).alias(Track.artists),
            pl.col(Track.release_date).drop_nulls().first(),
            pl.col(Track.region).drop_nulls().unique().sort(),
            pl.col(Track.country).drop_nulls().unique().sort(),
            pl.col(Playlist.id).n_unique().alias(Stats.playlist_count),
            pl.col(PlaylistOwner.name).n_unique().alias(Stats.dj_count),
        ).with_columns(
            pl.col(Track.region).list.filter(pl.element().ne('')).cast(pl.List(get_region_enum())),
            pl.col(Track.country).list.filter(pl.element().ne('')).cast(pl.List(get_country_enum())),
        ).unique().with_columns(
            pl.col(Track.artists).list.join(', ').alias(Track.artist_names),
            pl.col(Track.artists).list.eval(pl.element().str.to_lowercase().is_in(
                queer_artists)).list.any().alias(Track.has_queer_artist),
            pl.col(Track.artists).list.eval(pl.element().str.to_lowercase().is_in(
                poc_artists)).list.any().alias(Track.has_poc_artist),
        ).join(
            bpm_data.select(
                pl.col(Track.name).cast(TRACK_NAME_DTYPE),
                pl.col(Track.artist_names).cast(TRACK_ARTIST_DTYPE),
                pl.col('bpm').cast(TRACK_BPM_DTYPE).alias(Track.beats_per_minute)
            ).unique([Track.name, Track.artist_names]),
            how='left', on=[Track.name, Track.artist_names]
        )

    # Write pre-processed data to parquet file
    create_temp_dir()
    process_in_batches(
        typed_source_data,
        process_track_batch,
        batch_by=Track.id,
        batch_values=track_ids,
        batch_name="tracks",
        batch_size=10000,  # Higher batch sizes are faster but have a righer OOM risk
        output_name=TRACK_ORIGINAL_DATA_FILE if prepare_deduplication else TRACK_DATA_FILE,
    )

    ###################
    # PLAYLIST TRACKS #
    ###################

    playlist_tracks = typed_source_data.select(
        Playlist.id,
        Track.id,

        # The following metadata is not strictly required
        PlaylistTrack.number,
        PlaylistTrack.added_at,
    ).filter(
        pl.col(Playlist.id).is_not_null(),
        pl.col(Track.id).is_not_null(),
    ).group_by(Playlist.id, Track.id, PlaylistTrack.number).agg(
        # The source data contains duplicated entries with varying
        # metadata, possibly from different scaping runs.
        pl.col(PlaylistTrack.added_at).min()
    ).sort(Playlist.id, Track.id, PlaylistTrack.number)

    # Write pre-processed data to parquet file
    create_temp_dir()
    write_to_parquet_file(
        playlist_tracks,
        PLAYLIST_TRACKS_ORIGINAL_DATA_FILE if prepare_deduplication else PLAYLIST_TRACKS_DATA_FILE)


def process_song_duplicates(*, use_original_data: bool, print_statistics: bool = False):
    """Deduplicate songs based on track.name and track.artists.name."""

    # ===========================
    # Notes on song deduplication
    # ===========================
    #
    # Whereas Spotify considers the same song appearing on two different album
    # as two different tracks (with different track.id values),
    # we want to treat such instances only as a single track.
    #
    # =====================
    # Possible side effects
    # =====================
    #
    # --------------------------------
    # False Positives and/or Negatives
    # --------------------------------
    #
    # Due to data quality issues at Spotify's side, this will likely
    # still leave *some* songs that are effectively duplicates,
    # just with slightly different spellings of the title,
    # while maybe unifying some songs that are different recordings
    # which appear with the same name but on different albums.
    #
    # Most of these issues will likely mostly affect only older songs, though.
    #
    # NOTE: We could implement some safeguards, like checking for song length,
    #       once we have extended our data scraper to retrieve that data.
    #
    # ----------------------------------------
    # Unstable selection of canonical track.id
    # ----------------------------------------
    #
    # We also DO NOT currently implement an algorithm for deterministically
    # selecting one instance to be the "canonical" copy, but just use
    # the first one we stumble upon that has good metadata.
    #
    # This means that, within a single data preprocessing run, only
    # a single canonical track.id is selected for teach (name, artist)
    # pair, but which track.id is selected may vary between different runs.
    #
    # ==============
    # Other findings
    # ==============
    #
    # Within our dataset of (at that time) ~200,000 songs, a whopping ~86,000
    # were duplicates, that unified down to ~32,000 unique songs (plus the
    # ~114,000 songs that already had only a single copy).
    #
    # The most duplicated song had 22 duplicates, with others following closely behind.
    # Many duplicates were popular songs that were present in ~3,000 playlists each,
    # meaning that those duplicates definitely skewed the total statistics.
    #
    # There were ~75 songs with incomplete metdata that, on closer inspection,
    # seemed to not be songs but podcast episodes.

    songs_df = scan_parquet_file(
        TRACK_ORIGINAL_DATA_FILE if use_original_data else TRACK_DATA_FILE)

    def is_non_empty(expr): return expr.is_not_null() & expr.ne('')
    has_track_name = is_non_empty(pl.col(Track.name))
    has_track_artist = is_non_empty(pl.col(Track.artist_names))

    songs_without_track_name_and_artist = songs_df\
        .filter(~has_track_name & ~has_track_artist)\
        .select(Track.name, Track.artist_names, Track.id, Stats.playlist_count, Stats.dj_count)

    songs_without_track_name = songs_df\
        .filter(~has_track_name & has_track_artist)\
        .select(Track.name, Track.artist_names, Track.id, Stats.playlist_count, Stats.dj_count)

    # NOTE: Based on a quick look, the songs without an artist all seem to be podcasts
    # TODO: Pre-filter playlists by removing podcasts (this information is exposed by the Spotify API)
    songs_without_track_artist = songs_df\
        .filter(~has_track_artist & has_track_name)\
        .select(Track.name, Track.artist_names, Track.id, Stats.playlist_count, Stats.dj_count)

    duplicated_songs = songs_df\
        .filter(has_track_name & has_track_artist)\
        .group_by(Track.name, Track.artist_names)\
        .agg(pl.col(Track.id).unique().sort(),
             pl.col(Stats.playlist_count).sort(descending=True).alias(Stats.playlist_count),
             pl.col(Stats.dj_count).sort(descending=True).alias(Stats.dj_count),
             pl.col(Track.id).n_unique().alias('duplicate_count'),
             # This is only an estimate, because we would like playlists
             # which contain multiple different instances of the "same"
             # (by our definition) song to only be counted once.
             pl.col(Stats.playlist_count).sum().alias('estimated_total_playlist_count'))\
        .filter(pl.col('duplicate_count').gt(1))

    if print_statistics:
        print(songs_without_track_name_and_artist.collect(engine='streaming'))
        print(songs_without_track_name.collect(engine='streaming'))
        print(songs_without_track_artist.collect(engine='streaming'))

        print(duplicated_songs
              .sort('duplicate_count', descending=True)
              .collect(engine='streaming'))

        print(duplicated_songs
              .sort('estimated_total_playlist_count', descending=True)
              .collect(engine='streaming'))

        print(duplicated_songs.select(
            'duplicate_count').sum().collect(engine='streaming'))

    duplicate_to_canonical = duplicated_songs\
        .select(pl.col(Track.id).list.drop_nulls())\
        .select(pl.col(Track.id),
                pl.col(Track.id).list.first().alias('canonical.track.id'))\
        .explode(Track.id)\
        .sort(Track.id)

    if print_statistics:
        print(duplicate_to_canonical.collect(engine='streaming'))

    write_to_parquet_file(duplicated_songs.sort(
        [Track.name, Track.artist_names]), TRACK_DUPLICATES_DATA_FILE)
    write_to_parquet_file(duplicate_to_canonical, TRACK_CANONICAL_DATA_FILE)


def deduplicate_playlist_and_song_data():
    """Replace all duplicate tracks with their canonical versions."""

    playlists = scan_parquet_file(
        PLAYLIST_ORIGINAL_DATA_FILE)

    tracks_with_duplicates = scan_parquet_file(
        TRACK_ORIGINAL_DATA_FILE)

    playlist_tracks_with_duplicates = scan_parquet_file(
        PLAYLIST_TRACKS_ORIGINAL_DATA_FILE)

    duplicate_to_canonical = scan_parquet_file(
        TRACK_CANONICAL_DATA_FILE)

    playlist_tracks = playlist_tracks_with_duplicates\
        .join(duplicate_to_canonical, how='left', on=Track.id)\
        .with_columns(pl.col(Track.id).alias('duplicate.track.id'))\
        .with_columns(pl.col('canonical.track.id').fill_null(pl.col('duplicate.track.id')).alias(Track.id))\
        .drop('canonical.track.id', 'duplicate.track.id')\
        .group_by(Playlist.id, Track.id, PlaylistTrack.number).agg(
            # The source data contains duplicated entries with varying
            # metadata, possibly from different scaping runs.
            pl.col(PlaylistTrack.added_at).min())\
        .sort(Playlist.id, Track.id, PlaylistTrack.number)

    only_duplicates = duplicate_to_canonical\
        .filter(pl.col(Track.id).ne(pl.col('canonical.track.id')))

    track_statistics = playlist_tracks\
        .join(playlists.select(Playlist.id, PlaylistOwner.name), how='inner', on=Playlist.id)\
        .group_by(Track.id).agg(
            pl.col(Playlist.id).n_unique().alias(Stats.playlist_count),
            pl.col(PlaylistOwner.name).n_unique().alias(Stats.dj_count),
        )

    tracks = tracks_with_duplicates\
        .drop(Stats.playlist_count, Stats.dj_count)\
        .join(only_duplicates, how='anti', on=Track.id)\
        .join(track_statistics, how='left', on=Track.id)\
        .sort(Track.id)

    write_to_parquet_file(tracks, TRACK_DATA_FILE)
    write_to_parquet_file(playlist_tracks, PLAYLIST_TRACKS_DATA_FILE)


def compute_playlist_statistics():
    """Compute song_count and artist_count for all playlists."""
    playlists = scan_parquet_file(
        PLAYLIST_ORIGINAL_DATA_FILE)

    tracks = scan_parquet_file(
        TRACK_DATA_FILE)

    playlist_tracks = scan_parquet_file(
        PLAYLIST_TRACKS_DATA_FILE)

    track_count_per_playlist = playlist_tracks\
        .group_by(Playlist.id)\
        .agg(pl.col(Track.id).n_unique().alias(Stats.song_count))

    artist_count_per_playlist = playlist_tracks\
        .join(tracks, how='inner', on=Track.id)\
        .select(Playlist.id, Track.artist_names)\
        .group_by(Playlist.id)\
        .agg(pl.col(Track.artist_names).n_unique().alias(Stats.artist_count))

    playlists_with_stats = playlists\
        .drop(Stats.artist_count, Stats.song_count)\
        .join(track_count_per_playlist, how='inner', on=Playlist.id)\
        .join(artist_count_per_playlist, how='inner', on=Playlist.id)\
        .sort(Playlist.id)

    write_to_parquet_file(playlists_with_stats, PLAYLIST_DATA_FILE)


def process_playlist_tracks_inverse():
    """
    Create a separate copy of playlist_tracks that is optimized
    for track => playlist lookup, to enable certain query optimizations.
    """
    playlist_tracks = scan_parquet_file(PLAYLIST_TRACKS_DATA_FILE)

    track_playlists = playlist_tracks.sort(Track.id, Playlist.id, PlaylistTrack.number)

    write_to_parquet_file(track_playlists, TRACK_PLAYLISTS_DATA_FILE)


def process_song_lyrics():
    """Process the song lyrics into a table sorted by track.id"""
    temp_file = create_temp_dir() + 'temp_song_metadata_by_track_and_artist.parquet'

    print(f'Writing {temp_file}...')
    tracks = scan_parquet_file(TRACK_DATA_FILE)\
        .select(Track.id, Track.name, Track.artist_names)\
        .sort([Track.name, Track.artist_names])\
        .sink_parquet(temp_file)

    tracks = scan_parquet_file(temp_file)

    lyrics = scan_parquet_file(UNPROCESSED_TRACK_LYRICS_DATA_FILE)\
        .join(tracks,
              how='inner',
              left_on=['song', 'artist'],
              right_on=[Track.name, Track.artist_names])\
        .select(pl.col(Track.id),
                pl.col('lyrics').alias(TrackLyrics.lyrics))\
        .unique(Track.id)\
        .sort(Track.id)

    write_to_parquet_file(lyrics, TRACK_LYRICS_DATA_FILE)


def process_song_pairings():
    social_playlists = scan_parquet_file(PLAYLIST_DATA_FILE)\
        .filter(pl.col(Playlist.is_social_set))\
        .filter(~pl.col(Playlist.name).str.contains_any(['The Maine', 'delete', 'SPOTIFY']))\
        .select(Playlist.id)

    songs_df = scan_parquet_file(PLAYLIST_TRACKS_DATA_FILE)\
        .with_columns(pl.col(PlaylistTrack.number).cast(pl.Int64))\
        .join(social_playlists, how='semi', on=[Playlist.id])\
        .sort(Playlist.id, PlaylistTrack.number)\
        .rolling(index_column=PlaylistTrack.number, period='2i', group_by=Playlist.id)\
        .agg(pl.col(Track.id))\
        .filter(pl.col(Track.id).list.len().eq(2))\
        .group_by(pl.col(Track.id))\
        .agg(pl.col(Playlist.id).n_unique().alias(Stats.playlist_count))\
        .select(pl.col(Track.id).list.get(0).alias(TrackAdjacent.FirstTrack.id),
                pl.col(Track.id).list.get(1).alias(TrackAdjacent.SecondTrack.id),
                pl.col(Stats.playlist_count))\
        .filter(~pl.col(TrackAdjacent.FirstTrack.id).eq(pl.col(TrackAdjacent.SecondTrack.id)))\
        .sort([TrackAdjacent.FirstTrack.id, TrackAdjacent.SecondTrack.id])

    # Write pre-processed data to parquet files
    write_to_parquet_file(songs_df, TRACK_ADJACENT_DATA_FILE)


def process_playlist_and_song_tags():
    """Process the playlist names into tag tables for songs and playlists."""
    playlists = scan_parquet_file(PLAYLIST_DATA_FILE)
    playlist_tracks = scan_parquet_file(PLAYLIST_TRACKS_DATA_FILE)
    tracks = scan_parquet_file(TRACK_DATA_FILE)

    playlists_per_tag_limit: Final = 20
    TAG: Final = 'tag'

    playlists_tokenized = playlists.select(
        pl.col(Playlist.id),
        pl.col(Playlist.name),
        pl.col(Playlist.name).pipe(extract_tags_from_name).alias(PlaylistTags.tags),
    )

    exploded_playlists_by_tag = playlists_tokenized\
        .explode(PlaylistTags.tags)\
        .rename({PlaylistTags.tags: TAG})

    tags = exploded_playlists_by_tag\
        .group_by(TAG)\
        .agg(pl.col(TAG).count().alias(Tag.playlist_count),
             pl.col(Playlist.name).head(playlists_per_tag_limit).alias(Tag.playlist_names))\
        .select(pl.col(TAG).str.split(':').list.get(0).alias(Tag.category),
                pl.col(TAG).str.split(':').list.get(1, null_on_oob=True).alias(Tag.short_name),
                pl.col(TAG).alias(Tag.name),
                Tag.playlist_count,
                Tag.playlist_names)\
        .sort(Tag.playlist_count, descending=True)

    write_to_parquet_file(tags, TAGS_DATA_FILE)

    track_tags = playlist_tracks\
        .join(exploded_playlists_by_tag.rename({TAG: Tag.name})
              .filter(pl.col(Tag.name).is_not_null()),
              how='inner', on=Playlist.id)\
        .group_by(Track.id, Tag.name)\
        .agg(pl.col(Tag.name).count().alias(TrackTag.playlist_count))

    with TempFileTracker() as temp_files:
        temp_file = temp_files.register_for_deletion(TEMP_DATA_DIR + 'temp_track_tags.parquet')
        write_to_parquet_file(track_tags, temp_file)
        track_tags = scan_parquet_file(temp_file)

        temp_file = temp_files.register_for_deletion(TEMP_DATA_DIR + 'temp_track_tags_by_track_id.parquet')
        track_tags_by_track_id = track_tags.sort(Track.id)
        write_to_parquet_file(track_tags_by_track_id, temp_file)
        track_tags_by_track_id = scan_parquet_file(temp_file)

        def process_track_tags_batch(tracks_batch: pl.LazyFrame) -> pl.LazyFrame:
            return track_tags_by_track_id\
                .join(tracks_batch, how='semi', on=Track.id)\
                .group_by(Track.id)\
                .agg(pl.col(Tag.name).sort_by(Stats.playlist_count, descending=True).head(20),
                     pl.col(Stats.playlist_count).sort(descending=True).head(
                         20).alias(TrackTags.playlist_counts_per_tag),
                     pl.col(Stats.playlist_count).sort(descending=True).head(20).sum().alias(TrackTags.tag_relations_count))\
                .join(tracks_batch.select(Track.id, Track.name, Track.artists), how='inner', on=Track.id)

        def temp_file_for_index(index: int) -> str:
            return TEMP_DATA_DIR + f'temp_tag_batch_{index}.parquet'

        @with_temp_files
        def process_track_tags_in_batches(batch_temp_files: TempFileTracker):
            row_count = tracks.select(pl.len()).collect().item()
            batch_size = 10000  # Higher batch sizes are faster but have a righer OOM risk
            batch_count = int(math.ceil(row_count / batch_size))

            print(f"Processing {row_count:,} tracks in {batch_count:,} batches of {batch_size:,} items...")

            for batch_index in range(0, batch_count):
                batch_start = batch_index * batch_size
                print(f"Processing batch {batch_index:,}/{batch_count:,}")
                batch_result = process_track_tags_batch(tracks.slice(batch_start, batch_size))\
                    .sort('track.id')
                temp_file = batch_temp_files.register_for_deletion(temp_file_for_index(batch_index))
                write_to_parquet_file(batch_result, temp_file)

            print("Merging batches...")

            merged: pl.LazyFrame | None = None
            for batch_index in range(0, batch_count):
                batch_data = scan_parquet_file(temp_file_for_index(batch_index))
                merged = (batch_data if merged is None else
                          merged.merge_sorted(batch_data, 'track.id'))

            write_to_parquet_file(merged, TRACK_TAGS_DATA_FILE)

        process_track_tags_in_batches()


def process_everything(merge_duplicates: bool = True):
    """Runs all pre-processing in sequence."""
    # Reset the internal file tracker (only used for debugging)
    reset_file_tracker()

    # Extract country & region enums
    process_country_data()

    # Initial run to split playlists, tracks and playlist entries
    process_playlist_and_song_data(prepare_deduplication=merge_duplicates)

    # Duplicate song detection reuses the track data generated above
    process_song_duplicates(use_original_data=merge_duplicates)

    if merge_duplicates:
        deduplicate_playlist_and_song_data()
        compute_playlist_statistics()

    # Inverse track => playlist lookup reuses the (possibly deduplicated) data generated above
    process_playlist_tracks_inverse()

    # Song lyrics reuses the track data generated above
    process_song_lyrics()

    # Song pairings reuses the playlist entries data generated above
    process_song_pairings()

    # Extract tags from playlist titles and assign to songs
    process_playlist_and_song_tags()

    print("Done.")


# Run full pre-processing when invoked via `python preprocess.py`
if __name__ == '__main__':
    process_everything()
