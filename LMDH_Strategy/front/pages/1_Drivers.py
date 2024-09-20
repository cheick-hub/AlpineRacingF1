import streamlit as st
from streamlit_extras.switch_page_button import switch_page


from src.GlobalHandler import GlobalHandler
import front.pages.widgets.drivers as driver_widget

####################### LOAD CONFIGURATION #######################
if 'orchester' not in st.session_state:
    switch_page('main')

orchester: GlobalHandler = st.session_state["orchester"]
st.set_page_config(page_title='Drivers List', layout='wide', menu_items={
        'Report a bug': "mailto:dsi.performance@fr.alpineracing.com"
})
#####################################################

driver_widget.driver_form(orchester)
driver_widget.driver_info(orchester)
driver_widget.driver_list(orchester)