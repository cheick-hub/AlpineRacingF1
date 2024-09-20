import streamlit as st

from src.GlobalHandler import GlobalHandler
import front.callbacks.drivers as cb_driver
from src.Driver import DriverCategory
from src.utilities import format_time

def driver_form(orchester : GlobalHandler):
    with st.expander("Add a Driver", expanded=True):
        with st.form("from_add_driver", clear_on_submit = True):
            st.text_input(label="First Name", key='first_name_input', placeholder="Type a name...")
            st.text_input(label="Last Name", key='last_name_input', placeholder="Type a name...")
            st.text_input(label="Acronym", key='acronyme_input', placeholder="Type a name...")
            driver_categories = ["-"] + [member.value for _, member in DriverCategory.__members__.items()]
            st.selectbox("Category", options=driver_categories, key='category_input')

            st.form_submit_button("Add Driver", on_click=cb_driver.add_driver, args=[orchester])

@st.experimental_fragment
def driver_info(orchester : GlobalHandler):
    with st.expander("Driver Info", expanded=True):
        cols = st.columns(3)
        region_of_interest = cols[1] 
        with region_of_interest: 
            driver_selected = st.selectbox("Driver:", ["-"] + list(orchester.get_drivers().keys()), key='select_driver')
        
        if driver_selected == "-":
            with st.empty() : return

        driver = orchester.get_drivers()[driver_selected]
        cols = st.columns(3, gap="medium")
        with cols[0]:
            st.write(f"Driver: {driver.get_fullname()}")
            st.write(f"Total Driving Time: {format_time(driver.get_total_driving_time(), show_hour=True, show_milliseconds=False)}")
        with cols[1]:
            st.write(f"Acronym: {driver.get_acronym()}")
            st.write(f"Last 6h driving time: {driver.get_last_6h_driving_time()}") #TODO need to implement this method
        with cols[2]:
            st.write(f"Category: {driver.get_category()}")
            st.write(f"Number Stint Sessions: {driver.get_number_of_stint()}")


@st.experimental_fragment
def driver_list(orchester : GlobalHandler):
    with st.expander("Drivers List", expanded=True):
        driver_columns = ["FirstName", "LastName", "Acronym", "Category","Notes"]
        df_drivers = orchester.generate_dataframe_from_drivers(
        drivers_columns=driver_columns)

        st.data_editor(df_drivers, key="drivers_front",
                                        disabled=driver_columns[:-1],
                                        use_container_width=True, hide_index=True)
    