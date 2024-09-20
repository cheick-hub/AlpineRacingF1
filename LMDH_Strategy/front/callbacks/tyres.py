import streamlit as st
from src.GlobalHandler import GlobalHandler

def add_tyre(orchester : GlobalHandler):
    isNotNone = lambda x: not(x == None)
    set_input = st.session_state["set_input"]
    mileage_input = st.session_state["mileage_input"]
    compound_input = st.session_state["compound_input"]
    # add the rims
    front_left_rim_input = st.session_state["front_left_rim_input"]
    rear_left_rim_input = st.session_state["rear_left_rim_input"]
    front_right_rim_input = st.session_state["front_right_rim_input"]
    rear_right_rim_input = st.session_state["rear_right_rim_input"]

    all_fields_filled = isNotNone(set_input) and isNotNone(mileage_input) and isNotNone(compound_input) #\
                        # and isNotNone(front_left_rim_input) and isNotNone(rear_left_rim_input) \
                        # and isNotNone(front_right_rim_input) and isNotNone(rear_right_rim_input)
    
    if not all_fields_filled:
        st.toast("Please enter a Set name and select a compound")
        return 
    
    orchester.add_tyreset(set_name=set_input, mileage=mileage_input, compound=compound_input)
    st.toast("Set added successfully")
    orchester.load_tyreset()