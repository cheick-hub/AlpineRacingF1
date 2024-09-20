from time import sleep
import json
import yaml

from cache.RedisInteractor import RedisInteractor
from cache.cache_decorator import PATH_TO_CONF_FILE
from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum


class CacheLiveData():
    def __init__(self):
        with open(PATH_TO_CONF_FILE, 'r') as file:
            # Load the YAML data
            conf_file = yaml.safe_load(file)
        redis_params = {
            "host": conf_file["host"],
            "port": conf_file["port"],
            "database": conf_file["database"]
        }

        self.redis = RedisInteractor(**redis_params)
        if not self.redis.connect():
            raise ConnectionError(f'Connexion to {redis_params["database"]} failed')

    def something_to_do(self, var1, var2):
        print(f'Starting REDIS with {var1} and {var2}')
        sleep(2)   # Blocking
        print(f'Done : {var1} and {var2}')
        print(f"{'-'*40}")


    def insert_new_lapdata(self, 
                           lap_to_add: dict[str, any]) -> bool:
        """
        lap_to_add = {'RunUID' : str,
                      'LapNumber' : int,
                      'Variables' : {'name1': val1, 'name2': val2 ...}
                      }
        """

        def add_none_missing_laps(cached: dict,
                                  nb_cached_laps: int,
                                  lap_nb: int) -> dict:
            """
                Adds 'None' in case there are some missing laps. 
                This is temporary and will be replaced when the lapdata
                become avaliable.
            """
            cached.update({
                'Lap'+str(i): {'0': None} 
                for i in range(nb_cached_laps+1, lap_nb)
            })   # add None for missing laps if there are some missing
            return cached


        if not lap_to_add['Variables']:
            return   # nothing to do
        
        run_uid = lap_to_add['RunUID'].upper()
        lap_nb = lap_to_add['LapNumber']
        data_type = CatanaDataTypeEnum.LAPDATA
        lapdata = json.loads(lap_to_add['Variables'])   # {columns: [v1, v2], data:[1, 2]}
        lapdata = dict(zip(lapdata['columns'], lapdata['data'][0]))
        variables = list(lapdata.keys())

        cached_data = self.redis.get_cached_variable_from_runuid(
                                    data_type, run_uid, variables)

        for i, var in enumerate(variables):
            if var in cached_data['non_cached_variables']:   # create new field
                cached_lapdata = add_none_missing_laps({}, 0, lap_nb)
            else:   # load redis return 
                cached_lapdata = json.loads(cached_data['cached_results'][i])
                nb_cached_laps = len(cached_lapdata)
                cached_lapdata = add_none_missing_laps(cached_lapdata, 
                                                       nb_cached_laps, 
                                                       lap_nb)

            # add 'real' value or update if it was None
            lap_name = 'Lap' + str(lap_nb)
            if lap_name in cached_data and cached_lapdata[lap_name]['0']:
                print("Seems the lap was already inserted in the Redis cache",
                      f'for lapdata : {var}')
                continue
            
            cached_lapdata[lap_name] = {'0': lapdata[var]}
            lapdata[var] = cached_lapdata


        hash_ = run_uid + '+' + str(data_type)   # ok python 3.10 & 3.12
        formatted_insert = [(hash_, var, json.dumps(lapdata[var]))
                            for var in variables]
        
        inserted = self.redis.insert_ressource(formatted_insert, update=True)
        print("inserted in redis cache :", inserted)
        return inserted
        

            