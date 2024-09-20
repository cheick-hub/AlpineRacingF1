from datetime import datetime
import os
import pickle
import streamlit as st

from front.callbacks.loadings import save_state
from src.GlobalHandler import GlobalHandler
from src.Stint import Stint

def selected_driver():
    """
        Callback for the driver_choice widget
    """
    driver = st.session_state['driver_choice']
    st.session_state['stint_driver'] = driver

def selected_tyreset():
    """
        Callback for the tyreset_input widget
    """
    ts = st.session_state['tyreset_input']
    st.session_state['stint_tyreset'] = ts

def pitstop_time():
    """
        Callback for the pit_stop_time_input widget.
    """
    pit_time = st.session_state['pit_stop_time_input']
    st.session_state['pit_stop_time'] = pit_time

def fuel_added():
    """
        Callback for the fuel_pit_input widget.
    """
    fuel = st.session_state['fuel_pit_input']
    st.session_state['fuel_pit'] = fuel

def lap2grid_conso():
    """
        Callback for the cons_lap2grid_input widget
    """
    fuel2grid = st.session_state['cons_lap2grid_input']
    st.session_state['cons_lap2grid'] = fuel2grid

def formation_lap_nb():
    """
        Callback for the formation_lap_nb_input widget.
    """
    nb_form_lap = st.session_state['nb_formation_lap_input']
    st.session_state['nb_formation_lap'] = nb_form_lap

def formation_lap_conso():
    """
        Callback for the formation_lap_conso_input widget.
    """
    conso = st.session_state['formation_lap_conso_input']
    st.session_state['formation_lap_conso'] = conso

def race_start_time():
    """
        Callback for the race_starttime_input widget
    """
    start_time = st.session_state['race_starttime_input']
    st.session_state['race_starttime'] = start_time

def pop_front_session_state(variable_name: str) -> any:
    """
        Callback to remove a variable from the session state.
    """
    return st.session_state.pop(variable_name)

def create_new_stint(orchester: GlobalHandler):
    """
        Callback when the user clicked on the button to create a new stint.
        Assert alls fileds are corrects before creating the stint.
    """
    
    has_stint = orchester.has_stint()
    try:
        select_driver = st.session_state['stint_driver']
        select_set = st.session_state['stint_tyreset']

        if not has_stint:
            field_cons_to_grid_lap = pop_front_session_state('cons_lap2grid')
            field_nb_formation_lap = pop_front_session_state('nb_formation_lap')
            field_cons_to_form_lap = pop_front_session_state('formation_lap_conso')
            field_race_start_time = pop_front_session_state('race_starttime')
        else:
            select_pitstop_time = pop_front_session_state('pit_stop_time') 
            orchester.add_pit_stops(select_pitstop_time)
    except KeyError:
        handle_error('Please fill all fields to create a new stint')
        return
    
    fuel = 0 if not has_stint else st.session_state['fuel_pit']
    driver = orchester.get_driver(select_driver)
    tyre = orchester.get_set(select_set)
    add_stint_parameters = {
        "driver" : driver,
        "tyre" : tyre,
        "added_fuel" : fuel,
        "max_capacity" : st.session_state['capacity'],
        "has_stint" : has_stint,
        "track_length" : st.session_state['track_length'],
        "init_stint_parameters" : None if has_stint else {
            "cons_to_grid_lap" : field_cons_to_grid_lap,
            "nb_formation_lap" : field_nb_formation_lap,
            "cons_formation_lap" : field_cons_to_form_lap,
            "race_start_time" : field_race_start_time}
    }
    orchester.add_stint(**add_stint_parameters)
    st.session_state["selected"] = orchester.get_nth_stint(-1).get_stint_displayed_name()# added 19/07/27 cheick

def add_lap_to_stint(orchester: GlobalHandler, stint: Stint):
    """
        Callback when interacting with the 'laps' data_editor.
    """
    added_rows = st.session_state["laps"]['added_rows']
    if added_rows:
        if len(added_rows) > 1:
            e = IndexError("""Multiple laps where added at the same time.
                         Delete some lines to have only one 'unadded' line.
                         """)
            handle_error(e)
            return

        row_of_interest = added_rows[0]
        if row_of_interest:
            try:
                if 'Energy Lap (MJ)' in row_of_interest:
                    raise Exception("Cannot add Energy to a non existant lap, remove added line")
                input_ =row_of_interest['Input']
                orchester.add_lap(stint.stint_number, input_)
            except Exception as e:
                handle_error(e)
                return

    edited = st.session_state["laps"]['edited_rows']
    st.session_state['laps']['edited_rows'] = {}
    if edited:
        lap_idx = list(edited.keys())[0]
        if 'Energy Lap (MJ)' in edited[lap_idx]:
            cons = edited[lap_idx]['Energy Lap (MJ)']
            try:
                orchester.add_energy(stint.stint_number, lap_idx, cons)
            except Exception as e:
                handle_error(e)
                return

        elif 'Input' in edited[lap_idx]:
            edited_input = edited[lap_idx]['Input']
            try:
                orchester.edit_lap(stint.stint_number, lap_idx, edited_input)
            except Exception as e:
                handle_error(e)
                return
            
        elif 'Notes' in edited[lap_idx]:
            note = edited[lap_idx]['Notes']
            orchester.add_note(stint.stint_number, lap_idx, note)
        elif 'Comment' in edited[lap_idx]:
            comment = edited[lap_idx]['Comment']
            orchester.add_comment(stint.stint_number, lap_idx, comment)
    
        st.session_state['laps']['edited_rows'] = {}

    deleted_indexes = st.session_state["laps"]['deleted_rows']
    st.session_state['laps']['deleted_rows'] = []
    if deleted_indexes != []:
         for lap_idx in deleted_indexes:
            orchester.remove_lap(stint.stint_number, lap_idx)

    save_state()

    ###########################################################################

def change_start_time(stint: Stint):
    """
        Callback for the stint_start_time_input widget.
    """
    change_start = st.session_state['stint_start_time_input']
    if change_start != stint.start_time:
        stint.set_start_time(change_start)
        st.toast('Stint start time updated.')

def change_init_fuel(stint: Stint):
    """
        Callback function for the fuel_init_input widget.
    """
    fuel = st.session_state['fuel_init_input']
    if fuel != stint.fuel_init:
        stint.fuel_init = fuel
        st.toast('Initial stint fuel updated.')

def update_stint_weather(stint: Stint):
    """
        Callback for the stint_weather_input widget.
    """
    weather = st.session_state['stint_weather_input']
    if weather not in ('--', stint.track_weather):
        stint.track_weather = weather
        st.toast('Weather updated.')

def update_air_temp(stint: Stint):
    """
        Callback for the stint_air_input widget.
    """
    track_temp = st.session_state['stint_air_input']
    if track_temp not in ('--', stint.track_temp):
        stint.air_temp = track_temp
        st.toast('Track temperature updated.')

def update_track_state(stint: Stint):
    """
        Callback for the track_state_input widget.
    """
    track_state = st.session_state['track_state_input']
    if track_state not in ('--', stint.track_state):
        stint.track_weather = track_state
        st.toast('Track state updated.')

def update_track_temp(stint: Stint):
    """
        Callback for the track_temp_input widget.
    """

    tck_temp = st.session_state['track_temp_input']
    if tck_temp != stint.track_temp:
        stint.track_temp = tck_temp
        st.toast('Track temperature updated.')

def update_break_bias(stint: Stint):
    """
        Callback for the break_bias_input widget.
    """
    bb = st.session_state['break_bias_input']
    if bb != stint.break_bias:
        stint.break_bias = bb
        st.toast('Break bias updated.')

def handle_error(e):
    """
        Function to try to handle an error.
    """    
    st.session_state['error_message'] = e