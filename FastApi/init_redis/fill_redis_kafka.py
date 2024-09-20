### IMPORTS ################################################
import json
import time
import kafka
import logging
from utils.log_handler import CustomLogger
# from tqdm import tqdm
# from utils.log_handler import log 
from cache.cache_decorator import redis_params
from cache.RedisInteractor import RedisInteractor
# from parquet.CatanaDataTypeEnum import CatanaDataTypeEnum
# from parquet.CatanaAggregationEnum import CatanaAggregationEnum
# from parquet import LapData, RunData, CDCData, HistoData, Histo2DData, HistoLapData
from fill_redis import get_variables_from_path, is_valid_competition, insert_data

### MACROS #################################################
NB_SECONDS = 30
log_url = "http://127.0.0.1:8000"
path = r'/mnt/o/Symphony_tools/Webappauto/templates'
server = "vlt-k8s-master.provider.rsv.dir:9095"
topic = "dev_fill_redis"
group_id = "test_0"
db_params = redis_params

logging.setLoggerClass(CustomLogger)
logger : CustomLogger = logging.getLogger("J'aime les mandarines")
logger.set_url(log_url)
logger.configure_handlers(".")

Consumer = kafka.KafkaConsumer(topic, 
    bootstrap_servers=server, 
    group_id=group_id,
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    key_deserializer=lambda x: x.decode('utf-8'),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

### FUNCTIONS ##############################################

def validate_kafka_message(message : dict):
    """
    Validates the Kafka message.

    Args:
        message (dict): The Kafka message to validate.

    Returns:
        bool: True if the message is valid, False otherwise.
    """
    if "runuid" not in message:
        raise ValueError("The message does not contain the 'runuid' key")

    if "year" not in message:
        raise ValueError("The message does not contain the 'year' key")

    if "competition" not in message:
        raise ValueError("The message does not contain the 'competition' key")

    if not is_valid_competition(message["competition"]):
        raise ValueError("The competition is not valid")

    return

# def verify_insertion(interactor : RedisInteractor, runids : list[str], local_variables : list[str], datatype : str) -> None:
#     if datatype == 'CDC': return 
#     for runuid in runids:
#             data_is_inserted = interactor.verify_fields_from_ruuid(runuid=runuid, variables=local_variables, data_type=datatype)
#             if not(data_is_inserted): print(f"Error while inserting the data for the runuid '{runuid}' and the data type '{datatype}'")

# def process_page(interactor : RedisInteractor, competition : str, runuids : list[str], years: list[int], all_variables_dict : dict) -> None:
    
    # default_params = {"update" : True}
    # special_params = {"update" : True, "agg" : CatanaAggregationEnum.NONE}

    # mapper = {
    #         'LAPDATA': (LapData.LapData(competition=competition, variables= all_variables_dict.get('LAPDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.LAPDATA, default_params),
    #         'RUNDATA': (RunData.RunData(competition=competition, variables= all_variables_dict.get('RUNDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.RUNDATA, default_params),
    #         'CDCDATA': (CDCData.CDCData(competition=competition, variables= [], run_uid=runuids, years=years), CatanaDataTypeEnum.CDCDATA, special_params),
    #         'HISTODATA': (HistoData.HistoData(competition=competition, variables= all_variables_dict.get('HISTODATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTO, special_params),
    #         'MATRIXDATA': (Histo2DData.Histo2DData(competition=competition, variables= all_variables_dict.get('MATRIXDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTO2D, special_params),
    #         'HISTOLAPDATA': (HistoLapData.HistoLapData(competition=competition, variables= all_variables_dict.get('HISTOLAPDATA'), run_uid=runuids, years=years), CatanaDataTypeEnum.HISTOLAPDATA, special_params)
    #     }
    
    # for datatype in tqdm(mapper):
    #     logger.info(f"Processing the data for the data type '{datatype}'")
    #     class_, data_type, local_params = mapper[datatype]
    #     local_vars = all_variables_dict.get(datatype)
        
    #     if local_vars is None:
    #         logger.info(f"No variables found for the data type '{datatype}'")
    #         continue

    #     try:
    #         _ = class_.process_data(**local_params)
    #     except Exception as e:
    #         logger.error(f"Error while caching data for the data type '{datatype}'")
    #         logger.error(f"Error message: {e}")

    #     verify_insertion(interactor, runuids, local_vars, data_type)

# def insert_data(interactor : RedisInteractor, competition : str, runuids : list[str], years: list[int], all_variables_dict : dict) -> None:

    page_size = 20
    pages = [{"run": runuids[i:i + page_size], "year": years[i:i + page_size]} for i in range(0, len(runuids), page_size)]
    
    logger.info(f"{len(pages)} pages to process... Maxium runuids per page -> 20")
    process_page(interactor, competition, runuids, years, all_variables_dict)

### MAIN ###################################################
if __name__ == '__main__':
    while True:
        results = Consumer.poll(update_offsets=False)

        if not results:
            logger.info("No results")

        all_variables_dict = get_variables_from_path(path)    
        
        for _, msg_batch in results.items():
            for msg in msg_batch:
                try:
                    
                    validate_kafka_message(msg.value)
                    db_params["database"] = f"Catana{msg.value['competition']}"
                    rd_interactor = RedisInteractor(**db_params)
                    _ = rd_interactor.connect()
                    insert_data(rd_interactor, msg.value["competition"], msg.value["runuid"], msg.value["year"], all_variables_dict)
                    _ = rd_interactor.close()
                    Consumer.commit()
                
                except Exception as e:
                    logger.error(f"Error while processing the message : {e}")
                    partition = kafka.TopicPartition(msg.topic, msg.partition)
                    Consumer.seek(partition, msg.offset)

        # time.sleep(NB_SECONDS)
        time.sleep(10)


