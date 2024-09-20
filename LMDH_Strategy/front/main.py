import streamlit as st

import front.callbacks.loadings as load
import front.pages.widgets.home as home

import logging
import colorlog
import logging.handlers

logger = logging.getLogger('logs/main_log')   # peut être mieux à faire que fixer nom en dur
logger.setLevel(logging.INFO)

text_format = '%(log_color)s%(asctime)s | %(module)s:%(funcName)s | %(levelname)s | %(lineno)d |%(message)s '
file_format = '%(asctime)s | %(module)s:%(funcName)s | %(levelname)s | %(lineno)d |%(message)s '

file_handler = logging.handlers.TimedRotatingFileHandler('logs/log-',
                                                         when='midnight',
                                                         backupCount=14)
file_handler.setFormatter(logging.Formatter(file_format))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

log_info_console = logging.StreamHandler()
log_info_console.setLevel(logging.DEBUG)
log_info_console.setFormatter(colorlog.ColoredFormatter(text_format))
logger.addHandler(log_info_console)
 

st.set_page_config(page_title='Data', layout='wide',
                   menu_items={
        'Report a bug': "mailto:dsi.performance@fr.alpineracing.com"
})

if "orchester" not in st.session_state:
    load.init_app()



##################################### MAIN #####################################

st.title('Alpine Endurance Team')
st.markdown("""
            <style>
                div[data-testid="stExpander"] details summary p {
                    font-weight: bold;
                    font-size: 200%;
                }
            </style>
            """,
            unsafe_allow_html=True)

cols = st.columns(3)
with cols[1]:
    st.image('front/A424.jpg')

if ('load_req' not in st.session_state 
    or not st.session_state['load_req']):
    with cols[1]:
        st.button('Load an existing session', key='load_button',
                use_container_width=True, on_click=load.load_requested)
else:
    home.session_selector()
