import streamlit as st
from src.GlobalHandler import GlobalHandler
from src.Driver import Driver

def add_driver(orchester : GlobalHandler):
    first_name_input = st.session_state['first_name_input']
    last_name_input = st.session_state['last_name_input']
    acronyme_input = st.session_state['acronyme_input']
    category_input = st.session_state['category_input']
    
    mandatory_fields = (
            first_name_input and last_name_input and acronyme_input
            and (not (first_name_input.isspace() or last_name_input.isspace()
                or acronyme_input.isspace()))
            ) and category_input != "-"
    
    if not mandatory_fields: 
        st.toast("Please fill all the fields")
        return 
    
    orchester.add_driver(first_name=first_name_input,
                        last_name=last_name_input,
                        driver_acr=acronyme_input,
                        driver_cat=category_input)

    st.toast("Driver added successfully")
    orchester.load_drivers()

def get_driver(orchester : GlobalHandler) -> Driver:
    return orchester.get_drivers()[st.session_state['select_driver']]