import streamlit as st
from streamlit_extras.switch_page_button import switch_page

from front.callbacks.loadings import save_state
import front.pages.widgets.stint as widgets
import front.callbacks.stints as callback_stint

from src.GlobalHandler import GlobalHandler




if 'orchester' not in st.session_state:
    switch_page('main')

st.set_page_config(page_title='Stint handler',
                   layout='wide',
                   menu_items={
        'Report a bug': "mailto:dsi.performance@fr.alpineracing.com"
})


orchester: GlobalHandler = st.session_state["orchester"]

@st.experimental_dialog("Create a new stint")
def create_stint(orchester : GlobalHandler):
    """
        Pop up dialog to create a new stint.
        The widget shown are different if it is the first stint or not.
        Kept here as the rerun seems to work better here.
    """
    st.subheader(f"Stint : {orchester.get_nb_stint() + 1}")
    
    if ('error_message' in st.session_state 
        and st.session_state['error_message']):
        widgets.error_popup()

    is_first_stint = not orchester.has_stint()
    widgets.driver_list_widget(orchester)
    widgets.tyre_set_widget(orchester)

    if not is_first_stint:
        widgets.pistop_time_widget()
        widgets.fuel_added_widget(orchester)

    if is_first_stint:
        st.divider()
        widgets.cons_lap2grid_widget()
        widgets.cons_formation_lap_widget()
        widgets.nb_formation_lap_widget()
        st.divider()
        widgets.race_start_time_widget()

    if st.button('Create stint'):
        if not is_first_stint:
            save_state()
        callback_stint.create_new_stint(orchester)
        st.rerun()

def handle_select_change(): 
    st.session_state["selected"] = st.session_state["select_input"] 

create_select = 'Create a new stint'
default_select = '-'
options = [create_select, default_select] 
options.extend(orchester.generate_stint_names())

if not(st.session_state.get(['load_req'], False) or st.session_state['data_saved']):
    options = ["Can not create a stint", '-']
    st.error('Complete and save the Data page before creating stints.')

idx = options.index(st.session_state.get('selected', default_select))

with st.sidebar:
    if 'venue' in st.session_state:
        st.subheader( st.session_state['venue'])

    st.selectbox(
        label='Choose a stint',
        options=options,
        index=idx,
        key="select_input",
        on_change=handle_select_change,
    )

if st.session_state.get('selected', default_select) == create_select:   # ie. create new stint
    create_stint(orchester)
elif st.session_state.get('selected', default_select) in options[2:] :   # otherwise a point is printed on launch
    widgets.display_stint(orchester, st.session_state['selected'])