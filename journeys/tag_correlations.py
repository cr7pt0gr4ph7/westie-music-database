from collections.abc import Callable
from itertools import combinations

import networkx as nx
import streamlit as st
import polars as pl

from utils.search import TAG_CORRELATIONS_DATA_FILE, TAGS_DATA_FILE, TRACK_DATA_FILE, TRACK_TAGS_ORIGINAL_DATA_FILE
from utils.tables import Tag, Track, TrackTag, TrackTags


def immediate[R](func: Callable[[], R]) -> R:
    return func()


st.set_page_config(
    page_title="Tag Correlations",
    page_icon=":material/arrow_range:",
)

st.title("Tag Correlations")

r"""
Our computed tag data can be seen as a list of tuples $(\text{song}, \text{tag}, \text{confidence})$.
Based on that, we can define the likelihood that two tags occur together as the tuple
$(\text{tag1} \times \text{tag2}, \text{confidence})$.

"""

correlations = pl.scan_parquet(TAG_CORRELATIONS_DATA_FILE)
tracks = pl.scan_parquet(TRACK_DATA_FILE)
track_tags = pl.scan_parquet(TRACK_TAGS_ORIGINAL_DATA_FILE)
tags = pl.scan_parquet(TAGS_DATA_FILE)

correlation_methods = {
    "spearman_correlation": "Spearman Rank Correlation",
    "max_of_min": "max(min(song.tag1.count, song.tag2.count) for song in songs)",
    "sum_of_min": "sum(min(song.tag1.count, song.tag2.count) for song in songs)",
    "sum_of_sqrt_of_min": "sum(sqrt(min(song.tag1.count, song.tag2.count)) for song in songs)",
    "sum_of_cbrt_of_min": "sum(cbrt(min(song.tag1.count, song.tag2.count)) for song in songs)",
}


@immediate
@st.fragment
def tag_correlations():
    col1, col2 = st.columns(2)
    with col1:
        enable_correlations = st.toggle("Correlations")
    with col2:
        correlation_method = None
        if enable_correlations:
            correlation_method = st.selectbox("Correlation method", correlation_methods.keys(),
                                              index=list(correlation_methods.keys()).index("max_of_min"),
                                              label_visibility="collapsed",
                                              format_func=lambda option: f"Sort by: {correlation_methods.get(option)}")

    if enable_correlations:
        st.dataframe(correlations.sort(correlation_method, descending=True),
                     column_config={"tag1": st.column_config.TextColumn(pinned=True),
                                    "tag2": st.column_config.TextColumn(pinned=True)},
                     column_order=["tag1", "tag2",
                                   "spearman_correlation",
                                   "max_of_min",
                                   "sum_of_min",
                                   "sum_of_sqrt_of_min",
                                   "sum_of_cbrt_of_min"])


@immediate
@st.fragment
def tag_correlation_matrix():
    col1, col2 = st.columns(2)
    with col1:
        enable_correlation_matrix = st.toggle("Correlation Matrix")
    with col2:
        correlation_method = None
        if enable_correlation_matrix:
            correlation_method = st.selectbox("Correlation method", correlation_methods.keys(),
                                              index=list(correlation_methods.keys()).index("max_of_min"),
                                              label_visibility="collapsed",
                                              format_func=correlation_methods.get)

    tag_matrix = correlations\
        .sort('tag2', descending=True)\
        .collect()\
        .pivot('tag2',
               index='tag1',
               values=correlation_method,
               aggregate_function="first")\
        .sort('tag1')

    if enable_correlation_matrix:
        st.dataframe(tag_matrix)


@immediate
@st.fragment
def tag_comparison():
    if st.toggle("Tag Comparison"):
        tag_options = [
            "---",
            *tags
            .filter(Tag.category().is_in(["genre", "mood"]))
            .select(Tag.name)
            .sort(Tag.name)
            .collect()[Tag.name].to_list()
        ]

        col1, col2 = st.columns(2)
        with col1:
            tag1 = st.selectbox("Tag 1", tag_options)
        with col2:
            tag2 = st.selectbox("Tag 2", tag_options)

        if tag1 == "---":
            tag1 = None
        if tag2 == "---":
            tag2 = None
        tags_to_show = [tag1, tag2]

        if not (tag1 and tag2):
            tags_as_columns = pl.LazyFrame()
        else:
            filtered_track_tags = track_tags\
                .select(Track.id,
                        TrackTags.tags_data()
                        .list.filter(TrackTag.Tag.name.struct_field().is_in(tags_to_show)))\
                .filter(TrackTags.tags_data().list.len() == 2)

            # Explode each tag into a separate column that contains the tag's score
            tags_as_columns = filtered_track_tags\
                .explode(TrackTags.tags_data)\
                .select(Track.id, TrackTags.tags_data().struct.unnest())\
                .collect()\
                .pivot(TrackTag.tag,
                       index=Track.id,
                       values=TrackTag.matching_playlist_count,
                       aggregate_function="first")\
                .lazy()\
                .join(tracks.select(Track.id, Track.name, Track.artists), how='left', on=Track.id)\
                .select(Track.name, Track.artists, tag1, tag2)\
                .with_columns(pl.col(tag1, tag2).rank(method="dense").name.suffix("_rank"))\
                .with_columns((pl.col(f"{tag1}_rank", f"{tag2}_rank") / pl.col(f"{tag1}_rank", f"{tag2}_rank").max()).name.suffix("_percent"))

        st.dataframe(tags_as_columns)


@immediate
@st.fragment
def tag_clusters():
    tags_to_exclude = [
        "genre:fusion",
        "genre:remixes",
        "genre:duets",
        "genre:karaoke",
        "genre:covers",
        "genre:not blues",
        "genre:pop",
        "genre:indie",
        "genre:club",
        "genre:instrumental",
        "genre:guitar",
        "genre:piano",
        "genre:violins",
    ]

    related_clusters: list[list[str]] = [
        [
            "genre:late night",
            "genre:lo-fi",
        ],
        [
            "genre:beatless",
            "genre:late night",
            "genre:acoustic",
            # "genre:piano",
        ],
        [
            "genre:beatless",
            "genre:late night",
            "genre:acoustic",
            # "genre:guitar",
        ],
        [
            # "genre:instrumental",
            # "genre:guitar",
        ],
        [
            # "genre:instrumental",
            # "genre:piano",
        ],
        [
            # "genre:instrumental",
            # "genre:violins",
        ],
        [
            "genre:folk",
            "genre:country",
            # "genre:guitar",
        ],
        [
            "genre:motown",
            "genre:soul",
            "genre:funk",
            "genre:blues",
            "genre:bluesy",
        ],
        [
            "genre:soundtrack",
            "genre:classical",
            # "genre:instrumental",
            # "genre:violins",
        ],
        [
            "genre:jazz",
            # "genre:instrumental",
        ],
        [
            "genre:broadway",
            "genre:musicals",
            "genre:soundtrack",
            "genre:orchestral",
        ],
    ]

    # Additional clusters of opposite tags not contained in the data
    opposite_clusters: list[list[str]] = [
        [
            "genre:acoustic",
            "genre:beatless",
            "genre:late night",
            "genre:lo-fi",
            "genre:vaporwave",
            #
            "genre:blues",
            "genre:bluesy",
            "genre:funk",
            "genre:soul",
            "genre:motown",
            #
            "genre:bossa nova",
            "genre:latino",
            #
            "genre:country",
            "genre:folk",
            #
            "genre:techno",
            "genre:trance",
            #
            "genre:broadway",
            "genre:classical",
            "genre:musicals",
            "genre:orchestral",
            #
            "genre:dancehall",
            "genre:drum & bass",
            "genre:hip hop",
            "genre:jazz",
            "genre:kpop",
            "genre:punk",
            "genre:rap",
            "genre:r&b",
            "genre:rock",
            "genre:soundtrack",
        ],
        [
            "genre:musicals",
            "genre:classical",
        ]
    ]

    low_correlations = correlations\
        .filter(pl.col("max_of_min").is_between(0, 1))\
        .filter(pl.col("tag1").str.starts_with("genre:"),
                pl.col("tag2").str.starts_with("genre:"),
                ~pl.col("tag1").is_in(tags_to_exclude),
                ~pl.col("tag2").is_in(tags_to_exclude))

    edges = low_correlations.select("tag1", "tag2").collect().iter_rows()
    graph = nx.Graph(edges)

    graph.add_edges_from([pair
                          for cluster in related_clusters
                          for pair in combinations(cluster, 2)])

    graph.add_edges_from([pair
                          for cluster in opposite_clusters
                          for pair in combinations(cluster, 2)])

    max_cliques_list: list[list[str]] = list(nx.find_cliques(graph))
    max_cliques = pl.LazyFrame({"tags": max_cliques_list})

    exploded_cliques = max_cliques\
        .with_row_index(offset=1)\
        .explode("tags")

    sorted_cliques = exploded_cliques\
        .join(exploded_cliques
              .group_by("tags")
              .agg(pl.col("index").count().alias("count")),
              how="inner", on="tags")\
        .sort("count", "tags", descending=[True, False])\
        .group_by("index")\
        .agg("tags")\
        .sort("tags")\
        .with_columns(pl.col("tags").list.set_intersection(pl.col("tags").shift(-1, fill_value=pl.lit([]))).alias("same"),
                      pl.col("tags").list.set_difference(pl.col("tags").shift(-1, fill_value=pl.lit([]))).alias("added"),
                      pl.col("tags").shift(-1, fill_value=pl.lit([])).list.set_difference(pl.col("tags")).alias("removed"))\
        .with_columns(pl.row_index() + 1)

    st.dataframe(sorted_cliques, column_config={"tags": st.column_config.MultiselectColumn()})

    opposite_graph = nx.Graph()
    for pair in combinations(graph.nodes(), 2):
        if not graph.has_edge(*pair):
            opposite_graph.add_edge(*pair)

    max_cliques_list: list[list[str]] = list(nx.find_cliques(opposite_graph))
    max_cliques = pl.LazyFrame({"tags": max_cliques_list})

    exploded_cliques = max_cliques\
        .with_row_index(offset=1)\
        .explode("tags")

    sorted_cliques = exploded_cliques\
        .join(exploded_cliques
              .group_by("tags")
              .agg(pl.col("index").count().alias("count")),
              how="inner", on="tags")\
        .sort("count", "tags", descending=[True, False])\
        .group_by("index")\
        .agg("tags")\
        .sort("tags")\
        .with_columns(pl.col("tags").list.set_intersection(pl.col("tags").shift(-1, fill_value=pl.lit([]))).alias("same"),
                      pl.col("tags").list.set_difference(pl.col("tags").shift(-1, fill_value=pl.lit([]))).alias("added"),
                      pl.col("tags").shift(-1, fill_value=pl.lit([])).list.set_difference(pl.col("tags")).alias("removed"))\
        .with_columns(pl.row_index() + 1)

    st.dataframe(sorted_cliques, column_config={"tags": st.column_config.MultiselectColumn()})
