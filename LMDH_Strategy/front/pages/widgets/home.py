import streamlit as st

import front.callbacks.loadings as load


def choose_year():
    """
        Select a year 
    """
    years = load.get_years(st.session_state['data_path'])
    years = [y.split('/')[-1] for y in years]
    idx = None
    if 'year_chosen' in st.session_state:
        idx = years.index(st.session_state['year_chosen'])

    st.selectbox(label='Year', key = 'year_selected',
                 placeholder='Choose a year',
                 options=years, index=idx,
                 on_change=load.year_callback)


def choose_car():
    """
        Select a car 
    """
    path = st.session_state['data_path'] + st.session_state['year_chosen']
    cars = load.get_car_numbers(path)
    idx = None
    if 'car_number' in st.session_state:
        try:
            idx = cars.index(st.session_state['car_number'])
        except ValueError:
            idx = None

    if not cars:
        st.error('No data found for this year.')
    else:
        st.selectbox(label='Car number', key = 'car_selected',
                    placeholder="Choose a car",
                    options=cars, index=idx,
                    on_change=load.car_callback)

def choose_event():
    """
        Select an event.
    """
    path = st.session_state['data_path'] + st.session_state['year_chosen']
    path = path + '/' + st.session_state['car_number']

    events = load.get_events(path)
    idx = None
    if 'venue' in st.session_state:
        try:
            idx = events.index(st.session_state['venue'])
        except ValueError:
            idx = None

    if not events:
        st.error('No data found for this year for this car number.')
    else:
        st.selectbox(label='Event', key = 'event_selected',
                    placeholder="Choose an event",
                    options=events, index=idx,
                    on_change=load.event_callback)
        

@st.experimental_fragment
def session_selector():
    """
        Widgets to select the session to load if needed.
    """   
    cols = st.columns(3)
    with st.container(border=True):
        with cols[0]:
            choose_year()
        with cols[1]:
            if 'year_chosen' in st.session_state:
                choose_car()
        with cols[2]:
            if 'car_number' in st.session_state:
                choose_event()
    
    if 'venue' in st.session_state:
        with cols[1]:
            st.button(label='Load Event', on_click=load.load_event,
                      use_container_width=True)
