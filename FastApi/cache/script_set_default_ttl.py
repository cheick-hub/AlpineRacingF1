import os
import yaml
from tqdm import tqdm
from datetime import timedelta
from RedisInteractor import RedisInteractor

DEFAULT_TTL = timedelta(weeks=4)


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
PATH_TO_CONF_FILE = os.path.join(script_dir, "conf/db_conf.yaml")

# # Open the YAML file
with open(PATH_TO_CONF_FILE, 'r') as file:
    # Load the YAML data
    conf_file = yaml.safe_load(file)

redis_params = {
    "host": conf_file["host"],
    "port": conf_file["port"],
    "database" : "CatanaF1" # voir avec Tristan
    }


if __name__ == '__main__':
    # connect to redis
    db_interactor = RedisInteractor(**redis_params)
    connected = db_interactor.connect()
    # get all keys from redis interactor
    keys = db_interactor.connexion.keys()
    updated_keys_cpt = 0
    for name in tqdm(keys):
        updated_keys_cpt += db_interactor.connexion.expire(name, time=DEFAULT_TTL, nx=True)
    print(f"Default TTL set to 4 weeks {updated_keys_cpt} keys, {len(keys) - updated_keys_cpt} key(s) already had an expiry date")
