import streamlit as st

pg = st.navigation([
    st.Page("journeys/about_tagging.py",
            title="About Tagging",
            icon=":material/sell:"),
    st.Page("journeys/fast_and_slow.py",
            title="What is considered fast?",
            icon=":material/speed:"),
])
pg.run()
