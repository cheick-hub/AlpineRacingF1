import streamlit as st
from src.Compound import Compound
from src.GlobalHandler import GlobalHandler
import front.callbacks.tyres as cb_tyre

@st.experimental_fragment
def tyre_form(orchester : GlobalHandler):
    with st.form("from_add_tyre", clear_on_submit = True):
        st.write("Add a Set of Tyres")
        st.text_input(label="Set Name", placeholder="TS...", key="set_input")
        st.number_input("Initial mileage", value=0, min_value=0, key="mileage_input", placeholder="Type a number...")

        coumpound_categories = ["-"] + [member.value for _, member in Compound.__members__.items()]
        st.selectbox("Compound", coumpound_categories, key="compound_input", placeholder="Select a compound...")

        with st.expander('Rims', expanded=True):
            cols = st.columns(2, gap='medium')
            with cols[0]:
                st.number_input("FL", label_visibility="collapsed", min_value=None, value=None, key="front_left_rim_input", placeholder="FL")
                st.number_input("RL", label_visibility="collapsed", min_value=None, value=None, key="rear_left_rim_input", placeholder="RL")
            with cols[1]:
                st.number_input("FR", label_visibility="collapsed", min_value=None, value=None, key="front_right_rim_input", placeholder="FR")
                st.number_input("RR", label_visibility="collapsed", min_value=None, value=None, key="rear_right_rim_input", placeholder="RR")

        st.form_submit_button("Add Set", on_click=cb_tyre.add_tyre, args=[orchester])

    tyres_columns = ["set", "mileage", "compound", "notes"]
    df_tyres = orchester.generate_dataframe_from_tyres(tyres_columns=tyres_columns)
    st.data_editor(df_tyres, key="tyres_front",
                    disabled=["set"], use_container_width=True,
                    hide_index=True)
    