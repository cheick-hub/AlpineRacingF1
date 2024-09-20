import os
import time
from datetime import timedelta

import redis
import yaml

from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
from utils import singleton

ADD_TTL = timedelta(weeks=8)
DEFAULT_TTL = timedelta(weeks=4)


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
PATH_TO_CONF_FILE_MAPPING = os.path.join(script_dir, "conf/db_mapping.yaml")

# Open the YAML file
with open(PATH_TO_CONF_FILE_MAPPING, 'r') as file:
    # Load the YAML data
    db_mapping = yaml.safe_load(file)


class RedisInteractor(metaclass=singleton._Singleton):

    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = db_mapping[database]
        self.isConnected = False
        self.insertion_success = 0
        self.insertion_error = 0
        # implementer le fait qu'on ne peut pas effectuer une operatino si la connexion n'est pas établie

    def connect(self) -> bool:
            """
            Connects to the Redis server.

            Returns:
                bool: True if the connection is successful, False otherwise.
            """
            self.connexion = redis.Redis(host=self.host, port=self.port, db=self.database, decode_responses=True)
            self.isConnected = True
            return self.is_connected()

    def close(self) -> bool:
        """
        Closes the connection to the Redis server.

        Returns:
            bool: True if the connection is successfully closed, False otherwise.
        """
        self.isConnected = not (self.connexion.quit())
        return self.is_connected()

    def is_connected(self) -> bool:
            """
            Check if the RedisInteractor is currently connected to Redis.

            Returns:
                bool: True if connected, False otherwise.
            """
            return self.isConnected

    def insert_ressource(self, ressources: list[str, str, str], update: bool = False) -> bool:
        """
        Inserts a list of resources into Redis cache.

        Args:
            ressources (list[str, str, str]): A list of tuples containing the runuid, variable, and data for each resource.
            update (bool, optional): Specifies whether to update existing resources. Defaults to False.

        Returns:
            bool: True if the resources were successfully inserted or updated, False otherwise.
        """
        if not self.is_connected(): return False
        # get the current timestamp
        counter = 0
        ruuids = []
        for runuid, variable, data in ressources:
            variable_write = self.connexion.hset(name=runuid, key=variable, value=data)
            timestamp_write = self.connexion.hset(name=runuid, key=f"{variable}_timestamp",
                                                  value=str(round(time.time())))
            counter += bool(variable_write) + bool(timestamp_write)
            ruuids += [runuid]

        unique_runuids = set(ruuids)
        for runuid in unique_runuids:
            _ = self.connexion.expire(runuid, time=DEFAULT_TTL)
        return True if update else (counter / 2) == len(ressources)

    def delete_ressources(self, list_elements: list[str]) -> bool:
        """
        Deletes a list of resources from Redis cache.

        Args:
            list_elements (list[str]): A list of keys to delete from Redis.

        Returns:
            bool: True if all keys have been successfully deleted, False otherwise.
        """
        if not self.is_connected(): return False
        return self.connexion.delete(*list_elements)
        
    def clean_keys(self, keys: list[str]):
            """
            Deletes keys from Redis cache that match the given patterns.

            Args:
                keys (list[str]): A list of key patterns to match and delete.

            Returns:
                bool: True if all keys have been successfully deleted, False otherwise.
            """
            return_bool = True
            for key in keys:
                to_delete = list(self.connexion.scan_iter(match=f'*{key}*'))
                if to_delete == []:
                    continue
                _ = self.connexion.delete(*to_delete)
                # verify that the keys have been deleted
                to_delete = list(self.connexion.scan_iter(match=f'*{key}*'))
                return_bool &= (to_delete == [])
            return return_bool

    def __get_id_from_runuid(self, runuid: str, data_type: CatanaDataTypeEnum) -> str:
            """
            Returns the ID generated from the given runuid and data type.

            Args:
                runuid (str): The runuid to generate the ID from.
                data_type (CatanaDataTypeEnum): The data type to include in the ID.

            Returns:
                str: The generated ID.
            """
            return f"{runuid}+{data_type}"

    def get_runuid_from_id(self, id_: str) -> str:
            """
            Extracts the runuid from the given id.

            Args:
                id_: The id from which to extract the runuid.

            Returns:
                The extracted runuid.

            """
            return id_.split("+")[0]

    def format_get_data_response(self, data: dict[dict[str, list]], data_type: CatanaDataTypeEnum) -> list[str, str, str]:
        """
        Formats the data response obtained from Redis.

        Args:
            data (dict[dict[str, list]]): The data obtained from Redis, where the keys are runuids and the values are dictionaries of variables and their data.
            data_type (CatanaDataTypeEnum): The type of data being retrieved.

        Returns:
            list[str, str, str]: A list of tuples containing the formatted data, where each tuple consists of the runuid, variable, and variable data.

        """
        return_list = []
        for runuid in data:
            runuid_content = data[runuid]
            for variable, variable_data in runuid_content.items():
                return_list += [(self.__get_id_from_runuid(runuid, data_type), variable, variable_data)]
        return return_list

    def prepare_get_data_output(self, data: dict[str, dict[list, list, list]]) -> dict[str, dict[str, list]]:
        """
        Prepares the output data for the get_data method.

        Args:
            data (dict[str, dict[list, list, list]]): The input data containing cached variables and results.

        Returns:
            dict[str, dict[str, list]]: The prepared output data with cached variables as keys and their corresponding results as values.
        """
        return_dict = dict()
        for ruuid in data:
            return_dict[ruuid] = dict()
            for variable, variable_value in zip(data[ruuid]["cached_variables"], data[ruuid]["cached_results"]):
                return_dict[ruuid][variable] = variable_value

        return return_dict

    def get_cached_variable_from_runuid(self, data_type: CatanaDataTypeEnum, runuid: str, variables: list[str],
                                        update: bool = False) -> dict:
        """
        Retrieves cached variables and results from Redis based on the given runuid and variables.

        Args:
            data_type (CatanaDataTypeEnum): The data type of the runuid.
            runuid (str): The unique identifier for the run.
            variables (list[str]): The list of variables to retrieve from Redis.
            update (bool, optional): If True, forces an update of the cached variables. Defaults to False.

        Returns:
            dict: A dictionary containing the cached variables, cached results, and non-cached variables.

        """
        return_dict = {
            "cached_variables": [],
            "cached_results": [],
            "non_cached_variables": []
        }

        id_ = self.__get_id_from_runuid(runuid, data_type)
        # if runuid doesn't exist return dict in current state
        if update or not self.connexion.exists(id_):
            return_dict["non_cached_variables"] = variables.copy()
            return return_dict
        # update the ttl because the data is being used
        _ = self.connexion.expire(id_, time=ADD_TTL)
        # request redis for variables of runuid
        results = self.connexion.hmget(name=id_, keys=variables)
        # update return dict based on retrieved values
        for var_name, var_result in zip(variables, results):
            if var_result == None:
                return_dict["non_cached_variables"] += [var_name]
            else:
                return_dict["cached_variables"] += [var_name]
                return_dict["cached_results"] += [var_result]
                
        return return_dict

    def get_cached_variable_from_multiple_runuid(self, data_type: CatanaDataTypeEnum, dict_runUID: dict[str, list[str]], update: bool = False) -> dict:
        """
        Retrieves cached variables from multiple runuids.

        Args:
            data_type (CatanaDataTypeEnum): The data type of the variables to retrieve.
            dict_runUID (dict[str, list[str]]): A dictionary mapping runuids to lists of variables.
            update (bool, optional): Whether to update the cached variables. Defaults to False.

        Returns:
            dict: A dictionary mapping runuids to the cached variables.
        """
        return_dict = dict()

        for runuid in dict_runUID:
            variables = dict_runUID[runuid]
            cashed_result = self.get_cached_variable_from_runuid(data_type, runuid, variables, update)
            return_dict[runuid] = cashed_result

        return return_dict

    def get_last_modified(self, runuid: str, variables: list[str], data_type: CatanaDataTypeEnum) -> dict[str, int]:
            """
            Retrieves the last modified dates for the specified variables.

            Args:
                runuid (str): The unique identifier for the run.
                variables (list[str]): The list of variables to retrieve last modified dates for.
                data_type (CatanaDataTypeEnum): The data type of the variables.

            Returns:
                dict[str, int]: A dictionary mapping each variable to its last modified date.

            """
            id_ = self.__get_id_from_runuid(runuid, data_type)
            timestamps = self.connexion.hmget(name=id_, keys=[f"{var}_timestamp" for var in variables])
            return {var_name: int(timestamp) for var_name, timestamp in zip(variables, timestamps)}

    def verify_fields_from_ruuid(self, runuid: str, variables: list[str], data_type: CatanaDataTypeEnum) -> bool:
            """
            Verifies if the specified fields exist in the Redis hash associated with the given runuid.

            Args:
                runuid (str): The unique identifier for the run.
                variables (list[str]): The list of field names to verify.
                data_type (CatanaDataTypeEnum): The data type associated with the runuid.

            Returns:
                bool: True if all the fields exist in the Redis hash, False otherwise.
            """
            id_ = self.__get_id_from_runuid(runuid, data_type)
            if not self.connexion.exists(id_): return False
            return all([self.connexion.hexists(name=id_, key=var) for var in variables])
