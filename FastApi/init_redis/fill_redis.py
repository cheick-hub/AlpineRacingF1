### IMPORTS ################################################
import os
import pytz
import argparse
import datetime
import logging
import re

import pandas as pd
from tqdm import tqdm

from bdd import CATANA
from cache.cache_decorator import redis_params
from cache.RedisInteractor import RedisInteractor
from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from parquet.CatanaAggregationEnum import CatanaAggregationEnum
from parquet import LapData, RunData, CDCData, HistoData, Histo2DData, HistoLapData

### MACROS #################################################
path = r'/mnt/o/Symphony_tools/Webappauto/templates'
TIMEZONE = 'Europe/Paris'
file_path = './logs/last_executed_date.txt'


logs_create_directory = './logs/'
if not os.path.exists(logs_create_directory): os.makedirs(logs_create_directory)
# get the current date
current_date = datetime.datetime.now().strftime("%Y-%m-%d").replace("-", "_")
# create the logger
logging.basicConfig(
        filename=logs_create_directory + f'{current_date}_logs.log',\
        encoding='utf-8',
        format='%(asctime)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.INFO
    )

logger = logging.getLogger(__name__)
### FUNCTIONS ##############################################

def format_list_to_print(l: list, n_elem_per_line: int) -> str:
    """
    Formats a list into a string with a specified number of elements per line.

    Args:
        l (list): The list to be formatted.
        n_elem_per_line (int): The number of elements to be displayed per line.

    Returns:
        str: The formatted string.

    Example:
        >>> format_list_to_print([1, 2, 3, 4, 5, 6], 3)
        '1 2 3\n4 5 6\n'
    """
    str_ = ""
    for i in range(0, len(l), n_elem_per_line):
        str_ += ' '.join(map(str, l[i:i+n_elem_per_line])) + '\n'
    return str_

def extract_ruuids(competition: str, limitDate: str) -> list[str]:
    """
    Extracts the RunUIDs and corresponding years from the CATANA instance.

    Args:
        competition (str): The competition name.
        limitDate (str): The limit date.

    Returns:
        tuple: A tuple containing two lists - the list of RunUIDs (as strings) and the list of years (as strings).
    """
    catana_instance = CATANA.CATANA()
    data = catana_instance.get_new_processed_RUNUID(competition=competition, limitDate=limitDate)
    if data.empty: return [], []
    data['RunUID'] = data['RunUID'].astype('str')
    data['RunUID'] = data['RunUID'].str.upper()
    data.drop_duplicates(subset=['RunUID'], inplace=True)
    list_of_years = data['TimeOfRecording'].dt.strftime('%Y').to_list()
    list_of_uuids_non_processed = map(str, data['RunUID'].to_list())
    list_of_uuids_processed = [str(x) for x in list_of_uuids_non_processed]
    return list_of_uuids_processed, list_of_years

def extract_variables(x):
    """
    Extracts variables from a given string.

    Args:
        x (str): The input string.

    Returns:
        list(str): A list of extracted variables.

    """
    if "$" not in x:
        return [x]

    regex = r"\$(\w+)"

    matches = re.findall(regex, x)

    return matches

def get_variables_from_path(path: str) -> list[str]:
    """
    Retrieves variables from Excel files in the specified directory path.

    Args:
        path (str): The directory path containing the Excel files.

    Returns:
        dict: A dictionary where the keys are the source names and the values are lists of variables.

    """
    # get all the excel files in the directory path
    files = [(f, f.split(".")[0]) for f in os.listdir(path) if f.endswith('.xlsx')]

    all_excel_files = dict()
    all_excel_sheets = dict()
    for filepath, filename in files:
        # read the excel file and get the sheetnames as the filename
        excel_file = pd.ExcelFile(os.path.join(path, filepath))
        all_excel_sheets[filename] = excel_file.sheet_names
        all_excel_files[filename] = excel_file

    # for each excel and all sheetnames get columns of interesst
    all_df = []
    column_of_interest = ["Source", 'X Var', 'Y Var', 'Z Var']
    cpt = 0
    for filename, sheets in all_excel_sheets.items():
        for sheet in sheets:
            # read the sheet and add it in a df
            df = all_excel_files[filename].parse(sheet)
            local_columns = set(df.columns)
            have_info = (set(column_of_interest) & local_columns) == {'X Var', 'Source', 'Y Var', 'Z Var'}
            if not have_info: continue
            slice_ = df[column_of_interest]
            # all_df.append(slice_.dropna())
            all_df.append(slice_)

    # concatenate all the df
    df_interest = pd.concat(all_df)
    df_interest_ = df_interest[~df_interest['Source'].isna()]
    df_interest_ = df_interest_[df_interest_['Source'] != 'CONSTANT'].copy()
    df_interest_ = df_interest_.fillna('')

    return_dict = dict()

    for source in df_interest_.Source.unique():
        df_source = df_interest_[df_interest_['Source'] == source]
        x_ = [var for var_list in df_source['X Var'].apply(extract_variables) for var in var_list]
        y_ = [var for var_list in df_source['Y Var'].apply(extract_variables) for var in var_list]
        z_ = []
        if source == "MATRIXDATA":
            z_ = [var for var_list in df_source['Z Var'].apply(extract_variables) for var in var_list]
        set_to_add = set(x_ + y_ + z_)

        return_dict[source] = list(set_to_add)

    return return_dict

def is_valid_date(date: str) -> bool:
    """
    Check if a given date string is a valid date.

    Args:
        date (str): The date string to be checked.

    Returns:
        bool: True if the date string is a valid date, False otherwise.
    """
    try:
        pattern = '%Y-%m-%d %H:%M:%S' if len(date.split()) == 2 else '%Y-%m-%d'
        _ = datetime.datetime.strptime(date, pattern)
        return True
    except ValueError:
        return False

def is_valid_competition(competition: str) -> bool:
    """
    Check if the given competition is valid.

    Args:
        competition (str): The competition to check.

    Returns:
        bool: True if the competition is valid, False otherwise.
    """
    return competition in ["F1", "FE", "LMDh"]

def getLastExecutedDate() -> str:
    """
    Retrieves the last executed date from a file.

    If the file exists, it reads the last line of the file and returns the selected date.
    If the file does not exist, it creates the file and writes the current date and time, then returns the current date.

    Returns:
        str: The last executed date.
    """
    try:
        with open(file_path, 'r') as file:
            content = file.readlines()
            selected_date = content[-1].strip()
            assert is_valid_date(selected_date)
            return selected_date
    except FileNotFoundError:
        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # write the current now time in the file
        with open(file_path, 'w') as file:
            file.write(current_date + "\n")
            return current_date

def add_datatype_to_ruuids(list_of_ruuid : list[str]) -> list[str]:
    """
    Adds the datatype to the RunUIDs.

    Args:
        list_of_ruuid (list[str]): The list of RunUIDs.

    Returns:
        list[str]: The list of RunUIDs with the datatype added.
    """
    datatypes = ["CatanaDataTypeEnum.LAPDATA", "CatanaDataTypeEnum.RUNDATA", "CatanaDataTypeEnum.CDCDATA", "CatanaDataTypeEnum.HISTO", "CatanaDataTypeEnum.HISTO2D", "CatanaDataTypeEnum.HISTOLAPDATA"]
    return [f'{x}+{datatype}' for x in list_of_ruuid for datatype in datatypes]

def verify_insertion(interactor : RedisInteractor, runids : list[str], local_variables : list[str], datatype : str) -> None:
    if datatype == 'CDC': return 
    for runuid in runids:
            data_is_inserted = interactor.verify_fields_from_ruuid(runuid=runuid, variables=local_variables, data_type=datatype)
            if not(data_is_inserted): logger.error(f"Error while inserting the data for the runuid '{runuid}' and the data type '{datatype}'")

def process_page(interactor : RedisInteractor, competition : str, runuids : list[str], years: list[int], all_variables_dict : dict) -> None:
    
    default_params = {"update" : True}
    special_params = {"update" : True, "agg" : CatanaAggregationEnum.NONE}

    mapper = {
            'LAPDATA': (LapData.LapData(competition=competition, variables= all_variables_dict.get('LAPDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.LAPDATA, default_params),
            'RUNDATA': (RunData.RunData(competition=competition, variables= all_variables_dict.get('RUNDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.RUNDATA, default_params),
            'CDCDATA': (CDCData.CDCData(competition=competition, variables= [], run_uid=runuids, years=years), CatanaDataTypeEnum.CDCDATA, special_params),
            'HISTODATA': (HistoData.HistoData(competition=competition, variables= all_variables_dict.get('HISTODATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTO, special_params),
            'MATRIXDATA': (Histo2DData.Histo2DData(competition=competition, variables= all_variables_dict.get('MATRIXDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTO2D, special_params),
            'HISTOLAPDATA': (HistoLapData.HistoLapData(competition=competition, variables= all_variables_dict.get('HISTOLAPDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTOLAPDATA, special_params)
        }
    
    for datatype in tqdm(mapper):
        logger.info(f"Processing the data for the data type '{datatype}'")
        class_, data_type, local_params = mapper[datatype]
        local_vars = all_variables_dict.get(datatype)
        
        if local_vars is None:
            logger.info(f"No variables found for the data type '{datatype}'")
            continue

        try:
            _ = class_.process_data(**local_params)
        except Exception as e:
            logger.error(f"Error while caching data for the data type '{datatype}'")
            logger.error(f"Error message: {e}")

        verify_insertion(interactor, runuids, local_vars, data_type)

def insert_data(interactor : RedisInteractor, competition : str, runuids : list[str], years: list[int], all_variables_dict : dict) -> None:

    page_size = 20
    pages = [{"run": runuids[i:i + page_size], "year": years[i:i + page_size]} for i in range(0, len(runuids), page_size)]
    
    logger.info(f"{len(pages)} pages to process... Maxium runuids per page -> 20")
    process_page(interactor, competition, runuids, years, all_variables_dict)

### MAIN ###################################################
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--competition', type=str, help='Spécifier la competition', required=True)
    parser.add_argument('-d', '--date', type=str, help='Spécifier la date YYYY-MM-JJ', default=getLastExecutedDate())
    parser.add_argument('-p', '--path', type=str, help='Sepcifier le path des excels de calculs pour les variables',
                        default=path)
    args = parser.parse_args()

    if not is_valid_competition(args.competition): raise Exception(
        "La compétition saisie doit être dans l'ensemble {'F1', 'FE', 'LMDh'} ")
    if not is_valid_date(args.date): raise Exception(
        "La date saisie n'ai pas valide, Saisissez une date dans le bon format (YYY-MM-JJ)")

    logger.info("Start the data redis filling process")
    date_obj = datetime.datetime.strptime(args.date, '%Y-%m-%d %H:%M:%S')
    date_obj = date_obj.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(TIMEZONE))
    final_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
    update_date = datetime.datetime.now()
    list_of_ruuid, list_of_years = extract_ruuids(competition=args.competition, limitDate=final_date)
    
    logger.info(f"Extracted {len(list_of_ruuid)} runuids from the database with the competition '{args.competition}' and the limit date '{final_date}' (Timezone: {TIMEZONE})")
    if list_of_ruuid == []:
        logger.info("No runuids to process, exiting the process")
        exit(0)
    logger.info(f"List of runuids: \n{format_list_to_print(list_of_ruuid, 5)}")
    all_variables_dict = get_variables_from_path(args.path)
    
    db_params = redis_params
    db_params["database"] = "Catana" + args.competition
    # create an object to interact with Redis and connect to Redis
    rd_interactor = RedisInteractor(**db_params)
    _ = rd_interactor.connect()
    insert_data(rd_interactor, args.competition, list_of_ruuid, list_of_years, all_variables_dict)
    _ = rd_interactor.close()

    logger.info("End of the data redis filling process")
    # write the current now time in the file
    with open(file_path, 'a') as file:
        file.write(update_date.strftime("%Y-%m-%d %H:%M:%S") + "\n")
