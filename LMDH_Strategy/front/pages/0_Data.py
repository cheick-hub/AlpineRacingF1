import streamlit as st
from streamlit_extras.switch_page_button import switch_page

import front.pages.widgets.data as data_widget

if 'orchester' not in st.session_state:
    switch_page('main')

st.set_page_config(page_title='Data', layout='wide', menu_items={
        'Report a bug': "mailto:dsi.performance@fr.alpineracing.com"
})

st.markdown("""
            <style>
                div[data-testid="stExpander"] details summary p {
                    font-weight: bold;
                    font-size: 200%;
                }
            </style>
            """,
            unsafe_allow_html=True)

#######################################

data_widget.car_input()

with st.expander('Event', expanded=True):
    cols = st.columns(2, gap='medium')
    with cols[0]:
        data_widget.race_duration()
        data_widget.min_driving_time_input()

    with cols[1]:
        data_widget.venue_input()
        data_widget.track_input()

    cols_ = st.columns(3, gap='medium')
    with cols_[1]:
        data_widget.event_race_start_time()

with st.expander('Configurations', expanded=True):
    cols = st.columns(2, gap='medium')
    with cols[0]:
        data_widget.fuel_capacity_input()
        data_widget.density_input()
        data_widget.energy_stint_input()

    with cols[1]:
        data_widget.fuel_alarm_input()
        data_widget.oil_range_input()
        data_widget.energy_lap_input()

data_widget.save_button()
