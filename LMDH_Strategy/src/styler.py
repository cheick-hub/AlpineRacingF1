import seaborn as sns
TOTAL_LAP_COL_HEX = '#699996'
STINT_LAP_COL_HEX = '#ff0061'
LAP_TIME_COL_HEX = '#ff8a2e'
EMPTY_TIME_FIELD = "--:--.---"
FUEL_CONSUMED_COL_HEX = '#5020df'
MODIFIED_INPUT_COL = 'red'
CMAP = sns.light_palette("#fa9905", as_cmap=True)

def color_columns_text(color : str):
    return 'color: %s' % color

def color_columns_background(color : str):
    return 'background-color: %s' % color

color_total_lap = lambda  _ : color_columns_text(TOTAL_LAP_COL_HEX)
color_stint_lap = lambda  _ : color_columns_text(STINT_LAP_COL_HEX)
color_lap_time = lambda  _ : color_columns_text(LAP_TIME_COL_HEX)
color_fuel_consumed = lambda  _ : color_columns_text(FUEL_CONSUMED_COL_HEX)
color_edited_rows = lambda  _ : color_columns_text(MODIFIED_INPUT_COL)
