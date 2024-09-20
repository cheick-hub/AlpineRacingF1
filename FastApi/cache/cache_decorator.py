import os
import time  # temporaire

import yaml

from cache.RedisInteractor import RedisInteractor, CatanaDataTypeEnum

# CONFIGURATION ##########################################################################################################################################

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
PATH_TO_CONF_FILE = os.path.join(script_dir, "conf/db_conf.yaml")

# Open the YAML file
with open(PATH_TO_CONF_FILE, 'r') as file:
    # Load the YAML data
    conf_file = yaml.safe_load(file)

redis_params = {
    "host": conf_file["host"],
    "port": conf_file["port"],
}
# "database": conf_file["database"]
mapping_data_type_to_path = {
    CatanaDataTypeEnum.CDCDATA: "cdcdata",
    CatanaDataTypeEnum.HISTO2D: "histo2ddata",
    CatanaDataTypeEnum.HISTO: "histodata",
    CatanaDataTypeEnum.LAPDATA: "lapdata",
    CatanaDataTypeEnum.OTHER: "otherdata",
    CatanaDataTypeEnum.RUNDATA: "rundata"
}


# FUNCTIONS ################################################################################################################################################
def get_variable_tu_update(timestamps: dict, runuid: str, year: str, data_type: CatanaDataTypeEnum, path: str):
    """
    Retrieves the variables that need to be updated based on their timestamps.

    Args:
        timestamps (dict): A dictionary containing the timestamps of the variables.
        runuid (str): The unique identifier for the run.
        year (str): The year of the data.
        data_type (CatanaDataTypeEnum): The type of data.
        path (str): The path to the data storage.

    Returns:
        list: A list of variables that need to be updated.
    """
    path_header = path if path[-1] == "/" else path + "/"
    variable_to_decache = []
    for cache_variable in timestamps:
        variable_timestamp = timestamps[cache_variable]
        path_to_variable_parquet_storage = f"{path_header}{year}/{runuid}/computed_data/{mapping_data_type_to_path[data_type]}/{cache_variable}.parquet"
        start_time = time.time()
        last_modified_on_disk = os.path.getmtime(
            path_to_variable_parquet_storage)
        end_time = time.time()
        print(
            f"({runuid}, {variable_timestamp} -> timestamp={last_modified_on_disk} -> time_to_access={round(end_time - start_time, 5)}  path='{path_to_variable_parquet_storage}' ")
        if last_modified_on_disk > variable_timestamp:
            variable_to_decache.append(cache_variable)
    return variable_to_decache


def update_variable_to_check(redis_return_dict: dict, runuid: str, year: str, data_type: CatanaDataTypeEnum,
                             db_interactor: RedisInteractor, path: str):
    """
    Update the variables to check in the cache.

    Args:
        redis_return_dict (dict): A dictionary containing the cached variables.
        runuid (str): The unique identifier for the current run.
        year (str): The year for which the variables are being updated.
        data_type (CatanaDataTypeEnum): The type of data being updated.
        db_interactor (RedisInteractor): An object for interacting with the database.
        path (str): The path to the variables.

    Returns:
        list: A list of variables that were updated.

    """
    # interact with the database to get the list of variables with their timestamp
    local_variables = redis_return_dict[runuid]["cached_variables"]
    variables_timestamp = db_interactor.get_last_modified(
        runuid=runuid, variables=local_variables, data_type=data_type)
    variable_to_decache = get_variable_tu_update(
        variables_timestamp, runuid, year, data_type, path)
    if variable_to_decache == []:
        return variable_to_decache
    # remove the variable from the cache
    redis_return_dict[runuid]["cached_variables"] = [x for x in redis_return_dict[runuid]["cached_variables"] if
                                                     x not in variable_to_decache]
    redis_return_dict[runuid]["non_cached_variables"] += variable_to_decache
    return variable_to_decache


# DECORATOR ################################################################################################################################################
def rediscache(func):
    """
    A decorator function that caches the results of a function using Redis.

    Args:
        func: The function to be decorated.

    Returns:
        The decorated function.

    Raises:
        Exception: If there is a database connection error.

    """

    def wrapper(*args, **kwargs):
        # verify if we have required params
        assert (("data_type" in kwargs) and ("dict_runUID" in kwargs) and ("years" in kwargs) and (
                    "competition" in kwargs))
        # assert type(kwargs["data_type"]) == CatanaDataTypeEnum
        assert type(kwargs["dict_runUID"]) == dict
        assert len(kwargs["dict_runUID"]) == len(kwargs["years"])

        # solution de colmatage R05 devrait ne plus exister en R06
        list_of_runuids = list(kwargs["dict_runUID"].keys())
        # at the beginning all run_uids need the same variables
        variables_of_runuids = kwargs["dict_runUID"]
        if not list_of_runuids or not variables_of_runuids[list_of_runuids[0]]:
            return dict()

        db_params = redis_params
        db_params["database"] = "Catana" + kwargs["competition"]
        # create an object to interact with Redis and connect to Redis
        db_interactor = RedisInteractor(**db_params)
        connected = db_interactor.connect()

        # if not connected raise an exception
        if not connected:
            raise Exception("Database connection error")

        # if connected, get known info from cache
        data_type = kwargs["data_type"]
        years_of_runuids = kwargs["years"]

        # check if we need to update the cache
        update = kwargs["update"] if "update" in kwargs else False

        redis_return_dict = db_interactor.get_cached_variable_from_multiple_runuid(
            data_type=data_type,
            dict_runUID=kwargs["dict_runUID"],
            update=update
        )

        # get the missing variables for each runuid
        updated_dict_runUID = dict()
        updated_years = []
        for runuid, y_runuid in zip(redis_return_dict, years_of_runuids):
            needed_variable = redis_return_dict[runuid]["non_cached_variables"]
            if needed_variable == []:
                continue
            updated_dict_runUID[runuid] = redis_return_dict[runuid]["non_cached_variables"]
            updated_years.append(y_runuid)

        local_parameters = {
            "data_type": kwargs["data_type"],
            'dict_runUID': updated_dict_runUID,
            "years": updated_years,
            "update": update,
            "competition": kwargs["competition"]
        }

        # retrieve the data by calling the function
        computed_data = func(*args, **local_parameters)

        # format the computed data to a list of tuple (runuid+datatype, variable, variable_data)
        formatted_output = db_interactor.format_get_data_response(computed_data, kwargs["data_type"])

        # write the computed data to the database if needed
        if computed_data:
            all_inserted = db_interactor.insert_ressource(formatted_output)

        # update Redis dict to add the new computed && cached variable
        for id_, variable, data in formatted_output:
            runuid_extracted = db_interactor.get_runuid_from_id(id_)
            redis_return_dict[runuid_extracted]["cached_variables"] += [variable]
            redis_return_dict[runuid_extracted]["cached_results"] += [data]
            redis_return_dict[runuid_extracted]["non_cached_variables"] = [x for x in
                                                                           redis_return_dict[runuid_extracted][
                                                                               "non_cached_variables"] if x != variable]

        # reformat the dict with all cached and computed to the corresponding format based on the data_type
        final_output = db_interactor.prepare_get_data_output(redis_return_dict)
        return final_output

    return wrapper
