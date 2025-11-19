import streamlit as st

pg = st.navigation({
    "Tagging": [
        st.Page("journeys/about_tagging.py",
                title="Introduction: How we automatically tag Songs & Playlists",
                icon=":material/sell:"),
        st.Page("journeys/tag_correlations.py",
                title="Tag Correlations",
                icon=":material/arrow_range:"),
        st.Page("journeys/fast_and_slow.py",
                title="What is considered fast?",
                icon=":material/speed:"),
    ]
})
pg.run()
