"""Provides typed column name constants."""
from collections.abc import Collection
from typing import Final, Literal, Self

import polars as pl

from utils.common.entities import Entity, PolarsExpr, SubEntity, field
from utils.common.polars import sort_list_workaround


class Stats(Entity):
    artist_count: Final = field("artist_count", pl.UInt32)
    """
    The number of artists. Depending on the context, this can mean:

    1. The number of unique artists contained in a playlist.
    2. The number of unique artists contained in a DJ's playlists.
    3. The number of unique artists played in a certain region/country.
    """

    dj_count: Final = field("dj_count", pl.UInt32)
    """
    The number of DJs. Depending on the context, this can mean:

    1. The number of DJs that have this song in
       at least one of their playlists
    2. The number of DJs in a region/country.
    """

    playlist_count: Final = field("playlist_count", pl.UInt32)
    """
    The number of playlists. Depending on the context, this can mean:

    1. The number of playlists a song is in.
    2. The number of playlists a DJ has.
    3. The number of playlists in a certain region/country.
    """

    playlist_percent: Final = field("playlist_percent", pl.Float32)
    """
    The percentage of playlists that have a certain property, compared against a given total number of playlists.
    """

    song_count: Final = field("song_count", pl.UInt32)
    """
    The number of (unique) songs. Depending on the context, this can mean:

    1. The number of (unique) songs in a playlist.
    2. The number of (unique) songs in a DJ's playlists.
    3. The number of songs played in a certain region/country.
    """

    song_percent: Final = field("song_percent", pl.Float32)
    """
    The percentage of songs that have a certain property, compared against a given total number of songs.
    """

    date_formats: Final = field('date_formats', pl.List(pl.String))
    """
    The list of types of possible date strings. Depending on the context, this can mean:

    1. The list of types of possible date strings extracted from a playlist's name.
    2. The list of date format types used by a playlist owner.
    """

    date_format_counts: Final = field('date_format_counts', pl.List(pl.UInt32))
    """
    How often the different date format types occur in a user's playlists.
    """


class PlaylistOwner(Entity):
    """Represents a DJ who owns one or more playlists."""

    PREFIX: Final = "owner."
    """Common prefix for `PlaylistOwner` columns."""

    id: Final = field("owner.id", pl.String)
    """The Spotify User ID of the playlist's owner."""

    url: Final = field("owner.url", pl.String)
    """The Spotify User URL of the playlist's owner."""

    name: Final = field("owner.name", pl.String)
    """The name of the playlist's owner."""

    region: Final = field("owner.region", pl.Categorical)
    """
    The name of the world region a DJ is associated with.

    This is currently based on a manually curated dataset
    assigning DJs to their home regions/home countries.
    """

    country: Final = field("owner.country", pl.Categorical)
    """
    The name of the country a DJ is associated with.

    This is currently based on a manually curated dataset
    assigning DJs to their home regions/home countries.
    """

    is_wcs_dj: Final = field("owner.is_wcs_dj", pl.Boolean)
    """
    Whether this Spotify profile is known to belong to an actual WCS DJ.

    This is currently based on a manually curated dataset.
    """


class Playlist(Entity):
    """Represents a playlist (as retrieved from Spotify or from other sources)."""

    PREFIX: Final = "playlist."
    """Common prefix for `Playlist` columns."""

    id: Final = field("playlist.id", pl.String)
    """The Spotify ID of the playlist."""

    url: Final = field("playlist.url", pl.String)
    """The Spotify URL of the playlist."""

    name: Final = field("playlist.name", pl.String)
    """The name of the playlist."""

    extracted_dates: Final = field('playlist.extracted_date', pl.List(pl.String))
    """The list of possible date strings extracted from a playlist's name."""

    is_social_set: Final = field("playlist.is_social_set", pl.Boolean)
    """
    Whether this playlist likely represents an actual DJ set
    that was played (or is going to be played) at an event/party.
    """

    region: Final = field("playlist.region", pl.List(pl.Categorical))
    """
    The name of the world region a playlist (resp.
    the playlist's owner) is associated with.

    This is currently based on a manually curated dataset
    assigning DJs to their home regions/home countries.
    """

    country: Final = field("playlist.country", pl.List(pl.Categorical))
    """
    The name of the country a playlist (resp.
    the playlist's owner) is associated with.

    This is currently based on a manually curated dataset
    assigning DJs to their home regions/home countries.
    """

    matched_terms: Final = field("hit_terms", pl.List(pl.String))
    """The list of terms in the playlist's name that match the search query."""

    matched_terms_count: Final = field("hit_count", pl.UInt32)
    """The number of terms in the playlist's name that match the search query."""

    matching_playlist_count: Final = field("matching_playlist_count", pl.UInt32)
    """The number of playlists this track is in that also matched the search query."""

    matching_song_count: Final = field("matching_song_count", pl.UInt32)
    """The number of songs in this playlist that matched the search query."""

    matching_song_percent: Final = field("matching_song_percent", pl.Float32)
    """The percentage of songs in this playlist that matched the search query."""

    class Owner(SubEntity[PlaylistOwner], PlaylistOwner):
        """Represents the owner of a playlist."""
        pass


class PlaylistStats(Entity):
    id: Final = Playlist.id
    """The Spotify ID of the playlist."""

    wcs_song_count: Final = field('wcs_song_count', pl.UInt32)
    """How many likely-to-be-WCS tracks are contained in this playlist."""

    wcs_song_percent: Final = field('wcs_song_percent', pl.Float32)
    """How many percent of this playlist are likely-to-be-WCS tracks."""

    total_song_count: Final = field('total_song_count', pl.UInt32)
    """How many tracks are contained in this playlist."""


class Track(Entity):
    PREFIX: Final = "track."
    """Common prefix for `Track` columns."""

    id: Final = field("track.id", pl.String)
    """The Spotify ID of the song (`pl.String`)."""

    url: Final = field("track.url", pl.String)
    """The Spotify URL of the song."""

    name: Final = field("track.name", pl.String)
    """The name of the song."""

    artists: Final = field("track.artists", pl.List(pl.String))
    """The song's artists, represented as a list of artist names."""

    artist_names: Final = field("track.artists.name", pl.String)
    """The song's artist, represented as a single string."""

    has_queer_artist: Final = field("track.artists.is_queer_artist", pl.Boolean)
    """Whether any of the song's artist is known to be queer."""

    has_poc_artist: Final = field("track.artists.is_poc_artist", pl.Boolean)
    """Whether any of the song's artist is known to be be POC."""

    release_date: Final = field("track.album.release_date", pl.Date)
    """The song's release date."""

    beats_per_minute: Final = field("track.bpm", pl.Float64)
    """The song's tempo given as beats per minute."""

    region: Final = field("track.region", pl.List(pl.Categorical))
    """The list of world regions where a given track has been played."""

    country: Final = field("track.country", pl.List(pl.Categorical))
    """The list of countries where a given track has been played."""


class PlaylistTrack(Entity):
    PREFIX: Final = "playlist_track."
    """Common prefix for `PlaylistTrack` columns."""

    number: Final = field("playlist_track.number", pl.UInt16)
    """The index of this entry within the playlist."""

    added_at: Final = field("playlist_track.added_at", pl.Date)
    """The date when this entry was added to the playlist."""

    class Playlist(SubEntity[Playlist]):
        id: Final = Playlist.id

    class Track(SubEntity[Track]):
        id: Final = Track.id


class TrackAdjacent(Entity):
    times_played_together: Final = "times_played_together"

    class FirstTrack(SubEntity[Track]):
        id: Final = Track.id.alias("pair1.track.id")
        name: Final = Track.name.alias("pair1.track.name")
        artists: Final = Track.name.alias("pair1.track.artists")

    class SecondTrack(SubEntity[Track]):
        id: Final = Track.id.alias("pair2.track.id")
        name: Final = Track.name.alias("pair2.track.name")
        artists: Final = Track.artists.alias("pair2.track.artists")


class TrackLyrics(Entity):
    class Track(SubEntity[Track]):
        id: Final = Track.id

    lyrics: Final = field("track.lyrics", pl.String)
    """The full lyrics of a song."""

    matched_lyrics: Final = field("matched_lyrics", pl.List(pl.String))
    """The list of terms in the lyrics that match the search query."""

    matched_lyrics_count: Final = field("matched_lyrics_count", pl.UInt32)
    """The number of unique terms in the lyrics that match the search query."""


class Tag(Entity):
    """Represents an individual tag that can be applied to playlists and songs."""

    short_name: Final = field("tag", pl.String)
    """The name of the tag (without the category)."""

    category: Final = field("category", pl.String)
    """The category of the tag."""

    name: Final = field("full_tag", pl.String)
    """The full name of the tag (`i.e. `category:tag`)."""

    playlist_count: Final = Stats.playlist_count.alias("tag.playlist_count")
    """How many playlists have this tag."""

    max_playlist_count: Final = Stats.playlist_count.alias("max_playlist_count")
    """The maximum number of times a song has been tagged with this tag."""

    playlist_names: Final = Playlist.name.list()
    """The names of some (but not all) playlists that have this tag."""

    song_count = Stats.song_count
    """How many songs have this tag."""

    type CategoryName = str
    """Tag category name (e.g. `genre`, `events`)."""

    type ShortTagName = str
    """Short tag name without category (e.g. `pop`, `french open`)."""

    type TagName = str
    """Full tag name with category (e.g. `genre:pop`, `events:french open`)."""

    type SortFields = Literal["playlist_count", "song_count", "tag", "category", "full_tag"]
    """Fields that tags can be sorted on."""

    @staticmethod
    def format_tag(category: CategoryName, short_name: ShortTagName) -> TagName:
        return f'{category}:{short_name}'

    @staticmethod
    def format_tag_if_needed(category: CategoryName, short_name: ShortTagName) -> TagName:
        return (short_name if ':' in short_name
                else Tag.format_tag(category, short_name))

    @staticmethod
    def split_tag(name: TagName) -> tuple[CategoryName, ShortTagName | None]:
        parts = name.split(':', 1)
        return ((parts[0], parts[1]) if len(parts) >= 2
                else (parts[0], None))

    @staticmethod
    def format_tag_expr(category: PolarsExpr[CategoryName], short_name: PolarsExpr[ShortTagName]) -> PolarsExpr[TagName]:
        return pl.concat_str(category, pl.lit(':'), short_name)

    @staticmethod
    def split_tag_expr(tag_name: PolarsExpr[TagName]) -> pl.Expr:
        return tag_name\
            .cast(pl.String)\
            .str.splitn(':', 2)\
            .struct.rename_fields([Tag.category, Tag.short_name])

    @staticmethod
    def extract_category(tag_name: PolarsExpr[TagName]) -> PolarsExpr[CategoryName]:
        return Tag.split_tag_expr(tag_name).struct.field(Tag.category)

    @staticmethod
    def extract_tag(tag_name: PolarsExpr[TagName]) -> PolarsExpr[ShortTagName | None]:
        return Tag.split_tag_expr(tag_name).struct.field(Tag.short_name)


class TrackTag(Entity):
    """Represents the association between a single tag and a single track."""

    matching_playlist_count: Final = Stats.playlist_count.alias("matching_playlist_count")
    """How many playlists with the tag contain the track."""

    confidence: Final = field("confidence", pl.Float32)
    """How confident are we that this tag is correct between 0 and 1 (inclusive)."""

    tag: Final = Tag.name
    """The name of the tag."""

    @staticmethod
    def format_frequency():
        return pl.concat_str(
            TrackTag.tag.struct_field(),
            pl.lit(" ("),
            TrackTag.matching_playlist_count.struct_field(),
            pl.lit(")"))

    @staticmethod
    def format_confidence():
        return pl.concat_str(
            TrackTag.tag.struct_field(),
            pl.lit(" ("),
            TrackTag.confidence.struct_field(),
            pl.lit(")"))

    class Tag(SubEntity[Tag]):
        name: Final = Tag.name
        playlist_count: Final = Stats.playlist_count.alias("tag.playlist_count")
        playlist_percent: Final = field("tag.playlist_percent", pl.Float32)

    class Track(SubEntity[Track]):
        id: Final = Track.id
        playlist_count: Final = Stats.playlist_count.alias("track.playlist_count")
        playlist_percent: Final = field("track.playlist_percent", pl.Float32)


class TagsData:
    _data: pl.Expr

    def __init__(self, tags_data: pl.Expr | None = None):
        self._data = tags_data if tags_data is not None else TrackTags.tags_data()

    def filter(self, tag: Collection[str] | None = None, category: Collection[str] | None = None) -> Self:
        filters = []

        if tag is not None:
            filters.append(TrackTag.tag.struct_field().is_in(tag))

        if category is not None:
            filters.append(Tag.extract_category(TrackTag.tag.struct_field()).is_in(category))

        return TagsData(self._data.list.filter(*filters))

    def sort_by_frequency(self) -> Self:
        return TagsData(sort_list_workaround(
            self._data,
            pl.element().sort_by(TrackTag.matching_playlist_count.struct_field(),
                                 TrackTag.tag.struct_field(),
                                 descending=[True, False])))

    def compute_confidence_scores(self) -> Self:
        return TagsData(self._data.list.eval(
            pl.element().struct.with_fields(
                TrackTag.matching_playlist_count.struct_field().pipe(
                    lambda x: x/(x + 3)).alias(TrackTag.confidence.field_name))))

    def tags_count(self) -> pl.Expr:
        return self._data.list.len()

    def tags_data(self) -> pl.Expr:
        return self._data

    def alias(self, name: str) -> pl.Expr:
        return self._data.alias(name)

    def tags(self) -> pl.Expr:
        return self._data\
            .list.eval(pl.element().struct.field(TrackTag.tag))\
            .alias(TrackTags.tags)

    def playlist_counts_per_tag(self) -> pl.Expr:
        return self._data\
            .list.eval(pl.element().struct.field(TrackTag.matching_playlist_count))\
            .alias(TrackTags.playlist_counts_per_tag)

    def tags_with_frequencies(self) -> pl.Expr:
        return self._data\
            .list.eval(TrackTag.format_frequency())\
            .alias(TrackTags.tag_frequency)

    def tags_with_confidences(self) -> pl.Expr:
        return self._data\
            .list.eval(TrackTag.format_confidence())\
            .alias('tag_confidence')


class TrackTags(Entity):
    """Represents the tags of a song."""

    tags: Final = Tag.name.list().alias("tags")
    """The list of tags of the song."""

    playlist_counts_per_tag: Final = field("playlist_counts_per_tag", pl.List(pl.UInt32))
    """How often each tag is associated with this song. Has same length and order as `tags`."""

    tag_frequency: Final = field("tag_frequency", pl.List(pl.String))
    """Tags with associated playlist count formatted as a list of strings."""

    tags_data: Final = field("tags_data", pl.List(pl.Struct({
        TrackTag.tag: TrackTag.tag.field_type,
        TrackTag.matching_playlist_count: TrackTag.matching_playlist_count.field_type,
    })))
    """Combined information from `tags` and `playlist_counts_per_tag`."""

    tag_relations_count: Final = field("tag_relations_count", pl.UInt32)
    """The total number of `(Track=this_track, Tag, Playlist)` tuples. Same as `sum(playlist_counts_per_tag)`."""

    @staticmethod
    def unnest_tags_data():
        yield TrackTags.extract_tags()
        yield TrackTags.extract_playlist_counts_per_tag()

    @staticmethod
    def extract_tags(tags_data_expr: pl.Expr | None = None) -> pl.Expr:
        return TagsData(tags_data_expr).tags().alias(TrackTags.tags)

    @staticmethod
    def extract_playlist_counts_per_tag(tags_data_expr: pl.Expr | None = None):
        return TagsData(tags_data_expr).playlist_counts_per_tag().alias(TrackTags.playlist_counts_per_tag)


class PlaylistTags(Entity):
    """Represents the tags of a playlist."""

    id: Final = Playlist.id
    """The Spotify ID of the playlist."""

    tags: Final = Tag.name.list().alias("tags")
    """The list of tags of the playlist."""
