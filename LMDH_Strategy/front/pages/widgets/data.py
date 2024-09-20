from datetime import time, timedelta
import streamlit as st

import front.callbacks.data as cb_data


@st.experimental_fragment
def car_input():
    """
        Widget allowing to change which car is followed during the race.
    """
    opts = ['#35', '#36']
    idx = 0
    if 'car_number' in st.session_state:
        idx = opts.index(st.session_state['car_number'])
    else:
        st.session_state['car_number'] = opts[idx]

    with st.container(border=True):
        st.header('Car number')
        st.radio('Car number', label_visibility='collapsed',
                 key='car_nb_input', options=opts, index=idx, horizontal=True,
                 on_change=cb_data.car_change)

@st.experimental_fragment
def venue_input():
    """
        Text input widget for the venue name.
    """
    place_holder = 'Venue name'
    if 'venue' in st.session_state:
        place_holder = st.session_state['venue']
    st.text_input(label='Venue name', key='venue_input',
                  value=place_holder,
                  on_change=cb_data.venue_change)

@st.experimental_fragment
def race_duration():
    """
        Drop down selection widget for the race duration.
        TODO: change default value with a config file if one is added in
        the futur.
    """
    opts = ('-', 6, 8, 10, 24)
    idx = 0
    if 'race_duration' in st.session_state:
        idx = opts.index(st.session_state['race_duration'])

    st.selectbox(
        label = 'Race duration (h)', key = 'duration_input',
        options = opts, index=idx,
        on_change = cb_data.duration_change
    )

@st.experimental_fragment
def fuel_capacity_input():
    """
        Number input widget for the fuel capacity.
    """
    value = 0.0
    if 'capacity' in st.session_state:
        value = st.session_state['capacity']

    st.number_input(label='Fuel tank capacity (kg)', key = 'capacity_input',
                    min_value=0.0, max_value=90.0, step=0.1, value=value,
                    on_change=cb_data.capactiy_change)

@st.experimental_fragment
def fuel_alarm_input():
    """
        Number input widget for the fuel capacity.
    """

    value = 0
    if 'lap_alarm' in st.session_state:
        value = st.session_state['lap_alarm']
    else:
        st.session_state['lap_alarm'] = value

    st.number_input(label='Fuel Lap Alarm', key='fuel_lap_alarm',
                    min_value=0, max_value=20, step=1, value=value,
                    on_change=cb_data.fuel_alarm_change)

@st.experimental_fragment
def track_input():
    """
        Number input widget for the track length.
    """
    value = 0.0
    if 'track_length' in st.session_state:
        value = st.session_state['track_length']

    st.number_input(label='Track Length (km)', key='lenght_input',
                    min_value=0.0, max_value=20.0, step=0.01, value=value,
                    on_change=cb_data.track_lenght_change)

@st.experimental_fragment
def min_driving_time_input():
    """
        Slider input for the minimum driving time input. 
        TODO : If a race duration is set, gets it and sets the min driving time
        accordingly. 
        (contradiction excel vs. §13.2.4) & same experimental_fragment
    """
    min_time = time(1,0)
    max_time = time(6,00)
    if 'min_driving_time' in st.session_state:   # if one is present, both are
        min_time = st.session_state['min_driving_time']
        max_time = st.session_state['max_driving_time']
    else:
        st.session_state['min_driving_time'] = min_time
        st.session_state['max_driving_time'] = max_time
    
    st.slider(
        label='Possible driving time per driver (h)',
        key='driving_time_input',
        min_value=time(0,0), max_value=time(14,00),
        value=(min_time, max_time),
        step=timedelta(minutes=30),
        on_change=cb_data.driving_time_change
    )

@st.experimental_fragment
def oil_range_input():
    """
        Slider input for the oil level range input.
    """
    oil_min, oil_max = (3,5)
    if 'min_oil_level' in st.session_state:
        oil_min = st.session_state['min_oil_level']
        oil_max = st.session_state['max_oil_level']
    else:
        st.session_state['min_oil_level'] = oil_min
        st.session_state['max_oil_level'] = oil_max

    st.slider(
        label='Min/Max Oil level',
        key='oil_level_input',
        min_value=0, max_value=10,
        value=(oil_min, oil_max),
        step=1,
        on_change=cb_data.oil_level_input
    )

@st.experimental_fragment
def density_input():
    """
        Slider input for the fuel density
    """
    value = 1.0
    if 'fuel_density' in st.session_state:
        value = st.session_state['fuel_density']
    else :
        st.session_state['fuel_density'] = 1.0   # if no interaction w/ widget
    st.slider(
        label='Density', key='density_input',
        format='%.2f',
        min_value=0.5, max_value=2.0, step=0.01, value=value,
        on_change=cb_data.density_input
    )

@st.experimental_fragment
def energy_stint_input():
    """
        Number input for the energy stint
    """
    value = 900
    if 'stint_energy_sim' not in st.session_state:  # if widget not used
        st.session_state['stint_energy_sim'] = value
    else:
        value = st.session_state['stint_energy_sim']

    st.number_input(
        label='Energy per stint (MJ)', key='stint_energy_input',
        min_value=100, max_value=1500, value=value,
        on_change=cb_data.energy_stint_input
    )

@st.experimental_fragment
def energy_lap_input():
    """
        Number input for the energy per lap
    """
    value = 30.0
    if 'lap_energy_sim' not in st.session_state:  # if widget not used
        st.session_state['lap_energy_sim'] = value
    else:
        value = st.session_state['lap_energy_sim']

    st.number_input(
        label='Energy per lap (MJ)', key='lap_energy_input',
        min_value=10.0, max_value=100.0, value=value,
        on_change=cb_data.energy_lap_input
    )

@st.experimental_fragment
def event_race_start_time():
    value = time(11, 00)
    if 'event_race_start_time' not in st.session_state:
        st.session_state['event_race_start_time'] = value
    else:
        value = st.session_state['event_race_start_time']
    
    st.time_input(label="Race Start", key='event_race_start_time_input',
            value=value, step=15 * 60,
            on_change=cb_data.update_event_start_time)
    

@st.experimental_fragment
def save_button():
    """
        Button to save the event data. Switches from one car to the other
        if the car number has changed.
    """
    st.button('Save event data', use_container_width=True,
              on_click=cb_data.dump_event_data)
