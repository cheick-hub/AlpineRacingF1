import os
import streamlit as st
from streamlit_extras.switch_page_button import switch_page

from src.GlobalHandler import GlobalHandler
import front.pages.widgets.tyres as tyre_widget

if 'orchester' not in st.session_state:
    switch_page('main')

st.set_page_config(page_title='Tyre sets list', 
                   menu_items={
        'Report a bug': "mailto:dsi.performance@fr.alpineracing.com"
})

CURRENT_DIR = os.getcwd()
orchester : GlobalHandler = st.session_state["orchester"]
tyre_widget.tyre_form(orchester)
