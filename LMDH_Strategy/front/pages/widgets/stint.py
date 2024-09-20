from datetime import datetime
import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import pandas as pd

import src.styler as styler
import front.callbacks.stints as cb_stint
from front.callbacks.loadings import save_state
from src.GlobalHandler import GlobalHandler
from src.Stint import Stint

if 'orchester' not in st.session_state:
    switch_page('main')

def save_stint_button(st_: Stint):
    """
        Widget to save the orchester object.
    """
    df = st_.get_laps()
    if df.empty: 
        return
    
    cols_button = st.columns(7)
    with cols_button[3]:
        st.button('Valid Stint', on_click=save_state)

def error_popup():
    """
        Tests to handle error messages.
    """
    e = st.session_state['error_message']
    st.session_state['error_message'] = ''
    st.error(str(e))

@st.experimental_fragment
def driver_list_widget(orchester : GlobalHandler):
    """
        Selectbox widget listing the drivers.
    """
    st.selectbox("Driver :", key='driver_choice',
                 options=list(orchester.get_drivers().keys()),
                 index=None, on_change=cb_stint.selected_driver)
    
@st.experimental_fragment
def tyre_set_widget(orchester : GlobalHandler):
    """
        Selectbox widget listing the tyre sets avaliable.
    """
    st.selectbox("Set :", key='tyreset_input',
                 options=list(orchester.get_sets().keys()),
                 index=None, on_change=cb_stint.selected_tyreset)

@st.experimental_fragment
def pistop_time_widget():
    """
        Text widget for the pitstop duration.
    """
    st.text_input(label="Pit Stop", key='pit_stop_time_input',
                  placeholder="1.22 = 1 minute and 22 seconds",
                  on_change=cb_stint.pitstop_time)

@st.experimental_fragment
def fuel_added_widget(orchester : GlobalHandler):
    """
        Number input for the fuel added during the pit stop.
    """
    last_stint_registered = orchester.get_nth_stint(-1)
    remaining_fuel = last_stint_registered.get_remaining_fuel()
    max_possible = st.session_state["capacity"] - remaining_fuel
    max_possible = round(max_possible, 2) # be careful, see with thomas

    if 'fuel_pit' not in st.session_state:
        st.session_state['fuel_pit'] = max_possible


    st.number_input(label="Fuel added during the pitstop",
                    value=max_possible,
                    key='fuel_pit_input',
                    max_value=float(max_possible), min_value=0.0,
                    on_change=cb_stint.fuel_added)
    
@st.experimental_fragment
def nb_formation_lap_widget():
    """
        Number input for the number of formation laps.
    """
    default_value = 1
    if 'nb_formation_lap' not in st.session_state:
        st.session_state['nb_formation_lap'] = default_value

    st.number_input(label="Number of Formation Lap(s)",
                    key='nb_formation_lap_input',
                    min_value=default_value, step=1,
                    placeholder="lap(s)",
                    on_change=cb_stint.formation_lap_nb)

@st.experimental_fragment
def cons_lap2grid_widget():
    """
        Number input for the fuel used during the laps to the grid.
    """
    max_capa = st.session_state['capacity']

    st.number_input(label="Consumption Grid Lap(s) (kg)",
                    key='cons_lap2grid_input',
                    min_value=0.0, max_value=float(max_capa), step=0.1,
                    placeholder="kg",
                    on_change=cb_stint.lap2grid_conso)
    
@st.experimental_fragment
def cons_formation_lap_widget():
    """
        Number input for the formation lap(s).
    """
    max_capa = st.session_state['capacity']

    st.number_input(label="Consumption Formation Lap(s) (kg)",
                    key='formation_lap_conso_input',
                    min_value=0.0, max_value=float(max_capa), step=0.1,
                    placeholder="kg", on_change=cb_stint.formation_lap_conso)

@st.experimental_fragment
def race_start_time_widget():
    """
        Time input for the race start time.
    """
    # start_time = datetime.strptime("11:00:00",
                                #    "%H:%M:%S").time()
    start_time = st.session_state["event_race_start_time"]
    if 'race_starttime' not in st.session_state:
        st.session_state['race_starttime'] = start_time

    st.time_input(label="Race Start", key='race_starttime_input',
                  value=start_time, step=60,
                  on_change=cb_stint.race_start_time)

@st.experimental_fragment
def start_time_widget(orchester : GlobalHandler, stint: Stint):
    """
        Time input to set the stint start time.
    """
    with st.container(border=True):   
        st.subheader('Stint Start Time')
        st.time_input("Stint Start Time", key = 'stint_start_time_input',
                      value = stint.start_time,
                      step = 60,
                      label_visibility=('collapsed'),
                      disabled=orchester.has_stint(),
                      on_change=cb_stint.change_start_time,
                      args=[stint])

@st.experimental_fragment
def stint_driver_widget(stint: Stint):
    """
        Widget displaying the stint driver's accronym. The widget is 
        disabled on porpuse, no callback.
    """
    with st.container(border=True):   
        st.subheader('Driver')
        st.selectbox("Driver", key='stint_driver_input',
                     options = [stint.driver.get_acronym()],
                     disabled = True,
                     label_visibility='collapsed')

@st.experimental_fragment    
def stint_pitstop_widget(orchester : GlobalHandler, stint : Stint):
    """
        Display the pit stop time.
    """
    id_ = stint.get_stint_number() - 1 - 1 # minus 1 for index, minus 1 because first pitstop is for the second stint 
    pit_stop_time = str(orchester.get_pit_stop(id_))[2:] # a changer
    with st.container(border=True):   
        st.subheader('Pit Stop Time')
        st.selectbox('Pit Stop Time', key='pit_stop_time_display',
                    options=[pit_stop_time],
                    disabled = True,
                     label_visibility='collapsed')
        
@st.experimental_fragment
def fuel_stint_widget(orchester : GlobalHandler, stint: Stint):
    """
        Widget displaying the fuel at the begining of the stint.
    """
    fuel_start = st.session_state['capacity']#stint.get_fuel()

    with st.container(border=True):   # IN/OUT & Fuel
        st.subheader('Fuel at stint start')
        st.number_input(label='fuel', key='fuel_init_input',
                        value=fuel_start, min_value=0.0, step=0.1,
                        label_visibility='collapsed',
                        disabled=orchester.has_stint(),
                        on_change=cb_stint.change_init_fuel, args=[stint])
 
@st.experimental_fragment
def stint_air_temp(stint: Stint):
    """
        Widget to set/update the air temperature of the stint.
    """
    air_temp = stint.air_temp
    if air_temp is None:
        air_temp = 25.0

    st.number_input('Air Temp (°C)', key='stint_air_input',
                    value=stint.air_temp, step=1,
                    on_change=cb_stint.update_air_temp, args=[stint])

@st.experimental_fragment
def stint_track_weather(stint: Stint):
    """
        Widget to set/update the weather during the stint.
    """
    tck_weather = ['--', 'Sun', 'Cloud', 'Rain']
    idx = 0
    if stint.track_weather:
        idx = tck_weather.index(stint.track_weather)

    st.selectbox('Weather', key='stint_weather_input',
                 options = tck_weather,
                 index=idx, on_change=cb_stint.update_stint_weather,
                 args=[stint])

@st.experimental_fragment
def stint_track_state(stint: Stint):
    """
        Widget to set/update the track state during the stint.
    """
    tck_states = ['--', 'Dry', 'Damp', 'Wet']
    idx = 0
    if stint.track_state:
        idx = tck_states.index(stint.track_state)

    st.selectbox('Track State', key='track_state_input',
                 options=tck_states, index=idx,
                 on_change=cb_stint.update_track_state, args=[stint])
    
@st.experimental_fragment
def stint_track_temp(stint: Stint):
    """
        Widget to set/update the track temperature during the stint.
    """
    temp = 25
    if stint.track_temp is not None:
        temp = stint.track_temp

    st.number_input('Track Temp (°C)', key='track_temp_input',
                    value=temp, step=1,
                    on_change=cb_stint.update_track_temp, args=[stint])

def stint_weather_input(stint: Stint):
    """
        Widgets to input the stint weather.
        Weather, Air&Track Temp, Track State.
    """
    st.subheader('Stint Weather')
    cols = st.columns(4)
    with cols[0]:
        stint_track_weather(stint)

    with cols[1]:
        stint_air_temp(stint)

    with cols[2]:
        stint_track_state(stint)

    with cols[3]:
        stint_track_temp(stint)

@st.experimental_fragment
def break_bias_widget(stint: Stint):
    """
        Widget to set/update the break bias during the stint.
    """
    st.write('Break Bias (%)')
    bias = stint.break_bias
    if bias is None:
        bias = 0.0
    st.number_input(label='Break Bias', key='break_bias_input',
                    label_visibility='collapsed', value=bias,
                    on_change=cb_stint.update_break_bias, args=[stint])

@st.experimental_fragment
def fastest_lap_widget(stint: Stint):
    """
        Display the fastest lap of the stint.
    """
    fastest_lap = "--:--.---"
    
    if stint.get_nb_laps() > 0:
        fastest_lap = stint.get_fastest_lap_formated()
    
    with st.container(border=True):   
        st.subheader('Fatest Lap Time')
        st.selectbox('Fatest Lap Time', key='fastest_lap_display',
                     options=[fastest_lap],
                     disabled = True,
                     label_visibility='collapsed')

@st.experimental_fragment
def average_lap_widget(stint: Stint):
    """
        Display the average lap time of the stint.
    """
    average_lap = "--:--.---"
    
    if stint.get_nb_laps() > 0:
        average_lap = stint.get_average_lap_formated()
    
    with st.container(border=True):   
        st.subheader('Average Lap Time')
        st.selectbox('Average Lap Time', key='average_lap_display',
                    options=[average_lap],
                    disabled = True,
                     label_visibility='collapsed')

@st.experimental_fragment
def fuel_average_widget(stint: Stint):
    """
        Display the average fuel consumption of the stint.
    """
    average_fuel_computed = stint.get_average_fuel_consumption()
    average_fuel = round(average_fuel_computed, 2) if (average_fuel_computed is not None) else '--'
    
    with st.container(border=True):   
        st.subheader('Average Fuel Consumption')
        st.selectbox('Average Fuel Consumption', key='average_fuel_display',
                    options=[average_fuel],
                    disabled = True,
                     label_visibility='collapsed')

@st.experimental_fragment
def fuel_vs_tire_widget(stint: Stint):
    """
        Display the average fuel consumption of the stint.
    """
    
    fuel_flow = st.session_state['capacity'] / 40
    fuel_vs_tire_computed = stint.get_fuel_vs_tire(fuel_flow)
    fuel_vs_tire = f"{fuel_vs_tire_computed} s" if (fuel_vs_tire_computed is not None) else '--'
    
    with st.container(border=True):   
        st.subheader('Fuel vs Tyres')
        st.selectbox('Fuel vs Tyres', key='fuel_vs_tire_display',
                    options=[fuel_vs_tire],
                    disabled = True,
                     label_visibility='collapsed')
        
@st.experimental_fragment
def fuel_prediction_widget(stint: Stint):
    """
        Display the average fuel consumption of the stint.
    """
    fuel_pred = stint.get_fuel_prediction()
    fuel_prediction = f"{fuel_pred} lap(s)" if (fuel_pred is not None) else '--'
    
    with st.container(border=True):   
        st.subheader('Fuel Prediction')
        st.selectbox('Fuel Prediction', key='fuel_prediction_display',
                    options=[fuel_prediction],
                    disabled = True,
                     label_visibility='collapsed')
        
@st.experimental_fragment
def energy_average_widget(stint: Stint):
    """
        Display the average fuel consumption of the stint.
    """
    average_energy_computed = stint.get_average_energy_consumption()
    average_energy = round(average_energy_computed, 3) if (average_energy_computed is not None) else '--'
    
    with st.container(border=True):   
        st.subheader('Average Energy Consumption')
        st.selectbox('Average Energy Consumption', key='average_energy_display',
                    options=[average_energy],
                    disabled = True,
                     label_visibility='collapsed')

def car_config_widget(stint: Stint):
    """
        Display tyres set info and break bias input.
    """
    st.subheader('Stint Car Configuration')
    cols = st.columns(4)
    data = {
        'Tyre Set' : stint.TyreSet.get_set_name(),
        'Compound': stint.TyreSet.get_compound(),
        'Initial Tyre Milleage (km)':
            str(stint.TyreSet.get_initial_mileage())
    }
    for i, (key, val) in enumerate(data.items()):
        with cols[i]:
            st.write(key)
            st.write(val)
    with cols[3]:
        break_bias_widget(stint)

def display_stint_headers(orchester : GlobalHandler, stint: Stint):
    """
        Display the expander widget containing all the subwidgets
        representing the stint 'metadata'.
    """
    with st.expander("Stint Metadata", expanded=True):
        # First row
        nb_columns = 4 - int(stint.is_first_stint())

        cols = st.columns(nb_columns)
        with cols[0]:
            start_time_widget(orchester, stint)
        with cols[1]:   
            stint_driver_widget(stint)
        with cols[2]:
            fuel_stint_widget(orchester, stint)
        if not stint.is_first_stint():
            with cols[3]:
                stint_pitstop_widget(orchester, stint)

        # Second row
        with st.container(border=True):
            stint_weather_input(stint)

        # Third row
        with st.container(border=True):   # Stint Car Config
            car_config_widget(stint)

    with st.expander("Stint Statistics", expanded=True):
        # First row
        cols = st.columns(3)
        with cols[0]:
            fastest_lap_widget(stint)
            fuel_average_widget(stint)
        with cols[1]:
            average_lap_widget(stint)
            fuel_vs_tire_widget(stint)
        with cols[2]:
            energy_average_widget(stint)
            fuel_prediction_widget(stint)

def display_special_laps(stint_ : Stint):
    """
        Display the special laps (Lap2Grid & Formation) in a dataframe.
    """
    stint_id = stint_.get_stint_number() - 1
    is_first_stint = (stint_id == 0)

    if not is_first_stint: return 

    df_formation_and_to_grip_laps = stint_.get_special_laps()

    st.dataframe(df_formation_and_to_grip_laps,
                use_container_width=True,
                height=35*(len(df_formation_and_to_grip_laps))+37, hide_index=True)
    return 

@st.experimental_fragment
def display_stint(orchester : GlobalHandler, item: str):
    """
        DataEditor widget to display the stint and add laps.
    """
    # get the correct stint
    stint_to_display = item.split(' ')[1]
    stint_id = int(stint_to_display) - 1   # stint from 1, index from 0
    stint_ = orchester.Stints[stint_id]

    display_stint_headers(orchester, stint_)

    if ('error_message' in st.session_state 
        and st.session_state['error_message']):
        error_popup()

    all_stints_recap_widget(orchester)
    display_special_laps(stint_)
    display_laps(orchester, stint_)

def display_laps(orchester : GlobalHandler, stint_ : Stint):
    # Display Laps
    is_last_stint = (stint_.get_stint_number() == orchester.get_nb_stint()) 
    lap_columns = stint_.lap_columns
    disabled_columns = lap_columns[1:6]+lap_columns[7:9] + [lap_columns[10]]

    if not is_last_stint: disabled_columns = lap_columns[:-1]

    styled_df, size = design_df(stint_)
    
    text_columns_config = st.column_config.TextColumn()
    select_box_columns_config = st.column_config.SelectboxColumn(
        default = '',
        options = orchester.get_lap_types(),
    )
    conf_rows = 'dynamic' if is_last_stint else 'static'
    offset = 2 if is_last_stint else 0
    st.data_editor(styled_df,
                   disabled=disabled_columns,
                   key = 'laps',
                   column_config = { 
                       "Notes" : select_box_columns_config,
                       "Comments": text_columns_config,
                   },
                   use_container_width = True,
                   height = 35*(size + offset)+37,
                   hide_index = True,
                   num_rows=conf_rows,
                   on_change=cb_stint.add_lap_to_stint,
                   args=[orchester, stint_])

    if size == 0:
        cols_button = st.columns(7)
        with cols_button[3]:
            st.button('Valid Stint', on_click=save_state)
            
def design_df(stint_ : Stint):
    """
        Design the dataframe to be displayed.
    """
    df = stint_.Laps.reset_index(drop=True)
    edited_lap_idx = stint_.get_edited_laps()
    limit_energy = st.session_state['stint_energy_sim']
    limit_fuel = st.session_state['capacity']
        
    df_ = df.style.map(styler.color_total_lap, subset=['Total Lap'])\
        .map(styler.color_stint_lap, subset=['Stint Lap'])\
        .map(styler.color_lap_time, subset=['Lap Time', 'Time'])\
        .map(styler.color_fuel_consumed, subset=['Fuel Used (kg/lap)'])\
        .map(styler.color_fuel_consumed, subset=['Fuel Left'])\
        .map(styler.color_edited_rows, subset=pd.IndexSlice[edited_lap_idx, 'Input'])\
        .background_gradient(cmap=styler.CMAP, subset=['Energy Total (MJ)'], vmin=.6 * limit_energy, vmax=limit_energy)\
        .map(lambda x: 'background-color: transparent' if pd.isnull(x) else '')\
        .format(subset=['Fuel Used (kg/lap)', 'Fuel Left', 'Energy Lap (MJ)', 'Energy Total (MJ)', 'Tyre Mileage'], precision=2)\
        # .background_gradient(cmap=styler.CMAP, subset=['Fuel Left'], high=5, low=15)\
    # color updated
    return df_, len(df)

@st.experimental_fragment
def all_stints_recap_widget(orchester : GlobalHandler):
    # condition to display the widget, dont display if no laps for the first stint
    if orchester.get_nb_stint() == 1 and orchester.get_nth_stint(-1).get_nb_laps() == 0: 
        return
    # Display Laps
    stints_recap_df = orchester.generate_stint_recap() 
    columns_index_to_keep_editable = ["Fuel added", "Fuel margin", "Tire"]
    disabled_columns = list(set(stints_recap_df.columns) - set(columns_index_to_keep_editable))

    
    fuel_columns_config = st.column_config.NumberColumn(format="%.2f kg")
    fuel_added_columns_config = st.column_config.SelectboxColumn(
        default = '-', options = ['-', 'Full'])
    tire_columns_config = st.column_config.SelectboxColumn(
        default = '-', options = ['-', '1', '2', '3', '4'], )
   
    with st.expander("Stints Summary", expanded=False):
        st.data_editor(stints_recap_df,
                    disabled=disabled_columns,
                    key = 'stints',
                    column_config = { 
                        "Fuel added" : fuel_added_columns_config,
                        "Fuel margin": fuel_columns_config,
                        "Tire": tire_columns_config,
                        "Fuel InLap" : fuel_columns_config
                    },
                    use_container_width = True,
                    height = 35*(len(stints_recap_df))+37,
                    hide_index = True)

@st.experimental_fragment
def valid_stint(orchester : GlobalHandler, item: str):
    """
        Button to validate the stint.
    """
    # get the correct stint
    stint_to_display = item.split(' ')[1]
    stint_id = int(stint_to_display) - 1   # stint from 1, index from 0
    stint_ = orchester.Stints[stint_id]

    valid_stint_button = st.button('Valid Stint', use_container_width=True)
    if valid_stint_button:
        df : pd.DataFrame = stint_.get_laps()
        
        if df.empty:
            return
        
        save_state()
        st.success(f'Stint {stint_.get_stint_number()} saved.')
    