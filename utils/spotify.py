from collections.abc import Iterable
from itertools import batched
from typing import Final

import polars as pl
import spotipy
import streamlit as st

from streamlit_oauth import OAuth2Component
from utils.tables import Track

# Set OAuth parameters
REDIRECT_URI: Final = "http://127.0.0.1:8501"
SPOTIFY_TOKEN: Final = "sptf_token"


def is_spotify_integration_configured() -> bool:
    return bool(st.secrets.get('spotify')
                and st.secrets['spotify'].get('client_id')
                and st.secrets['spotify'].get('client_secret'))


def spotify_login_button():
    oauth_scopes = [
        'playlist-read-private',
        'playlist-modify-private',
        'playlist-modify-public',
    ]

    oauth2 = OAuth2Component(
        client_id=st.secrets['spotify']['client_id'],
        client_secret=st.secrets['spotify']['client_secret'],
        authorize_endpoint=spotipy.SpotifyOAuth.OAUTH_AUTHORIZE_URL,
        token_endpoint=spotipy.SpotifyOAuth.OAUTH_TOKEN_URL,
        refresh_token_endpoint=spotipy.SpotifyOAuth.OAUTH_TOKEN_URL,
    )

    # Check if token exists in session state
    if SPOTIFY_TOKEN not in st.session_state:
        # If not, show authorize button
        result = oauth2.authorize_button("Log in with Spotify", REDIRECT_URI, " ".join(oauth_scopes))
        if result and 'token' in result:
            # If authorization successful, save token in session state
            st.session_state[SPOTIFY_TOKEN] = result.get('token')
            st.rerun()
    else:
        # If token exists in session state, show the token
        token = st.session_state[SPOTIFY_TOKEN]
        with st.expander(":white_check_mark: **Login successful!** \u2e3a Click here for details for nerds :wink:"):
            st.json(token)
            if st.button("Refresh Token"):
                # If refresh token button is clicked, refresh the token
                token = oauth2.refresh_token(token)
                st.session_state[SPOTIFY_TOKEN] = token
                st.rerun()


def create_spotify_client() -> spotipy.Spotify | None:
    # Check if token exists in session state
    if SPOTIFY_TOKEN not in st.session_state:
        # If not, we aren't logged in
        return None
    else:
        # Otherwise we have a token and can initialize a Spotify client
        # TODO: Integrate in a way that allows for automatic token refresh
        return spotipy.Spotify(
            auth=st.session_state[SPOTIFY_TOKEN]['access_token'],
        )


def create_spotify_playlist(
    spotify_client: spotipy.Spotify,
    name: str,
    description: str,
    tracks: pl.DataFrame | Iterable[str],
) -> str:
    if isinstance(tracks, pl.DataFrame):
        track_urls = tracks[Track.url].to_list()
    else:
        track_urls = tracks

    user_id = spotify_client.current_user()['id']
    playlist_info = spotify_client.user_playlist_create(
        user=user_id,
        name=name,
        public=False,
        collaborative=False,
        description=description,
    )
    playlist_id = playlist_info['id']
    playlist_url = playlist_info['external_urls']['spotify']

    # Spotify accepts at most 100 items per request
    for track_batch in batched(track_urls, 100):
        spotify_client.playlist_add_items(playlist_id, track_batch)

    return playlist_url
