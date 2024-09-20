from datetime import datetime
import glob
import os
import pickle
import streamlit as st
import yaml

from src.GlobalHandler import GlobalHandler

def init_app():
    """
        Function to create the global handler when opening the application.
    """
    st.session_state['data_path'] = "data/"
    st.session_state['data_saved'] = False
    data_path = st.session_state['data_path']  # a optimiser
    path_conf_file = "configuration/conf.yaml"
    with open(path_conf_file, 'r', encoding='UTF-8') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    orchester = GlobalHandler()
    orchester.add_tyre_path(data_path + config["path_to_tyreset_data"])
    orchester.add_driver_path(data_path + config["path_to_driver_data"])
    orchester.load_tyreset()
    orchester.load_drivers()
    st.session_state["orchester"] = orchester

def load_requested():
    st.session_state['load_req'] = True
    st.session_state['data_saved'] = True

def get_years(path: str) -> list[str]:
    """
        Function to get a list of all the years having an event.
        The year the event took place should start with a '2'.
    """
    dirs = glob.glob(f'{path}/2*')
    dirs.sort(reverse=True)
    return dirs

def get_car_numbers(path: str) -> list[str]:
    """
        List cars in the given path. Car number should start with '#'.
    """
    cars = glob.glob(f'{path}/#*')
    cars = [c.split('/')[-1] for c in cars]
    cars.sort()
    return cars

def get_events(path: str) -> list[str]:
    events = glob.glob(f'{path}/*')
    events = [e.split('/')[-1] for e in events]
    return events

def year_callback() -> None:
    """
        Saves the chosen year to the session state.
    """
    chosen = st.session_state['year_selected']
    st.session_state['year_chosen'] = chosen


def car_callback() -> None:
    """
        Saves the chosen car to the session state.
    """
    chosen = st.session_state['car_selected']
    st.session_state['car_number'] = chosen

def load_data(event_path: str) -> None:
    """
        Load the 'Data' saved for a given event.
    """
    try:
        with open(f'{event_path}/orchester.pkl', 'rb') as handle:
            unserialized_data = pickle.load(handle)

        st.session_state['orchester'] = unserialized_data
    except FileNotFoundError:
        st.error('No Stint found for this event.')

def load_event():
    """
        Function to load an event when one is selected.
    """
    path = st.session_state['data_path'] + st.session_state['year_chosen']
    path += '/' + st.session_state['car_number']
    path += '/' + st.session_state['venue']

    with open(f'{path}/data.pkl', 'rb') as f:
        data:dict = pickle.load(f)

    for key, value in data.items():
        st.session_state[key] = value

    load_data(path)

def event_callback() -> None:
    """
        Saves the chosen event to the session state.
    """
    chosen = st.session_state['event_selected']
    st.session_state['venue'] = chosen
    load_event()

def save_state():
    """
        Callback to save the stint in a file.
    """

    path = st.session_state['data_path']
    if 'year_chosen' in st.session_state:
        year = st.session_state['year_chosen']
    else:
        year = str(datetime.now().year)

    car = st.session_state['car_number']
    event = st.session_state['venue']
    path = f'{path}{year}/{car}/{event}'
    if not os.path.exists(path):
        os.makedirs(path)   # creates dirs if not existing

    with open(f'{path}/orchester.pkl', 'wb') as f:
        pickle.dump(st.session_state['orchester'], f,
                    protocol=pickle.HIGHEST_PROTOCOL)
        