from datetime import datetime
import os
import pickle
import streamlit as st

def car_change():
    """
        Callback for the car_nb_input widget.
    """
    car_nb = st.session_state['car_nb_input']
    if car_nb != st.session_state['car_number']:
        st.session_state['car_number'] = car_nb
        st.toast(f'Session now following car {car_nb}')

def venue_change():
    """
        Callback for the venue_input widget
    """
    venue:str = st.session_state['venue_input']
    if venue.isspace():
        st.error('Missing venue name')
    elif 'venue' not in st.session_state:
        st.session_state['venue'] = venue
        st.toast('Venue name added')
    elif st.session_state['venue'] != venue:
        st.session_state['venue'] = venue
        st.toast('Venue name updated')

def duration_change():
    """
        Callback for the duration_input widget
    """
    duration = st.session_state['duration_input']
    if not isinstance(duration, int):
        return
    if 'race_duration' not in st.session_state:
        st.session_state['race_duration'] = duration
        st.toast('Race duration added')
    elif duration != st.session_state['race_duration']:
        st.session_state['race_duration'] = duration
        st.toast('Race duration updated')

def track_lenght_change():
    """
        Callback for the lenght_input widget
    """
    length = st.session_state['lenght_input']
    if length < 1:
        st.toast('Enter a track lenght of at least 1km.')
    elif 'track_length' not in st.session_state:
        st.session_state['track_length'] = length
        st.toast('Track length added')
    elif st.session_state['track_length'] != length:
        st.session_state['track_length'] = length
        st.toast('Track length updated')

def capactiy_change():
    """
        Callback for the capacity_input widget
    """
    capacity = st.session_state['capacity_input']
    if capacity == 0:   #TODO : make it temporary
        st.error('Enter a non-null capacity')
    elif 'capacity' not in st.session_state:
        st.session_state['capacity'] = capacity
        st.toast('Fuel capacity added')
    elif st.session_state['capacity'] != capacity:
        st.session_state['capacity'] = capacity
        st.toast('Fuel capacity updated')

def update_event_start_time():
    """
        Callback for the event_start_time widget
    """
    start_time = st.session_state['event_race_start_time_input']
    st.session_state['event_race_start_time'] = start_time
    st.toast('Event start time updated')

def energy_lap_input():
    """
        Callback for the lap_energy_input widget
    """
    lap_energy = st.session_state['lap_energy_input']
    st.session_state['lap_energy_sim'] = lap_energy
    st.toast('Lap energy used for the simulation updated.')

def fuel_alarm_change():
    """
        Callback for the fuel_lap_alarm widget
    """
    alarm = st.session_state['fuel_lap_alarm']
    if alarm == 0:
        st.toast("The alarm is set to 0 lap.")
    if 'lap_alarm' not in st.session_state:   # if not elif on purpose
        st.session_state['lap_alarm'] = alarm
        st.toast('Fuel lap alarm added')
    elif st.session_state['lap_alarm'] != alarm:
        st.session_state['lap_alarm'] = alarm
        st.toast('Fuel lap alarm updated')

def driving_time_change():
    """
        Callback for the driving_time_input widget
    """
    min_time, max_time = st.session_state['driving_time_input']
    if 'min_driving_time' not in st.session_state:
        st.session_state['min_driving_time'] = min_time
        st.session_state['max_driving_time'] = max_time
        st.toast('Driving time added')
    elif min_time != st.session_state['min_driving_time']:
        st.session_state['min_driving_time'] = min_time
        st.toast('Minimum driving time updated')
    elif max_time != st.session_state['max_driving_time']:
        st.session_state['max_driving_time'] = max_time
        st.toast('Maximum driving time updated')

def oil_level_input():
    """
        Callback for the oil_level_input widget
    """

    min_level, max_level = st.session_state['oil_level_input']

    if 'min_oil_level' not in st.session_state:
        st.session_state['min_oil_level'] = min_level
        st.session_state['max_oil_level'] = max_level
        st.toast('Min/Max Oil level added')
    elif min_level != st.session_state['min_oil_level']:
        st.session_state['min_oil_level'] = min_level
        st.toast('Minimum oil level updated')
    elif max_level != st.session_state['max_oil_level']:
        st.session_state['max_oil_level'] = max_level
        st.toast('Maximum oil level updated')

def density_input():
    """
        Callback for the density_input widget.
    """

    density = st.session_state['density_input']
    if 'fuel_density' not in st.session_state:
        st.session_state['fuel_density'] = density
        st.toast('Fuel density added')
    elif density != st.session_state['fuel_density']:
        st.session_state['fuel_density'] = density
        st.toast('Fuel density updated')

def energy_stint_input():
    """
        Callback for the stint_energy_input widget
    """
    stint_energy = st.session_state['stint_energy_input']
    st.session_state['stint_energy_sim'] = stint_energy
    st.toast('Max stint energy added.')

def dump_event_data():
    """
        Save Event data as a pickle dict.
    """
    to_save = ['car_number', 'venue', 'race_duration', 'track_length',
              'capacity', 'lap_alarm', 'min_driving_time', 'max_driving_time',
              'min_oil_level', 'max_oil_level', 'fuel_density', 
              'stint_energy_sim', 'lap_energy_sim', 'event_race_start_time']
    try:
        # data = {k: st.session_state[k] for k in to_save}
        data = {}
        for k in to_save:
            data[k] = st.session_state[k]
    except KeyError:
        st.error('Please fill all the fields before saving. Missing field: ' + k)
        return
    
    year = str(datetime.now().year)
    path = f"data/{year}/{data['car_number']}/{data['venue']}/"
    if not os.path.exists(path):
        os.makedirs(path)   # creates dirs if not existing

    file_path = path + 'data.pkl'
    with open(file_path, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    st.session_state['data_saved'] = True
    st.toast('Event data saved')