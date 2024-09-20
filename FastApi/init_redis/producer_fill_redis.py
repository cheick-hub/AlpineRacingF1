import kafka
import json
from uuid import uuid4

encoding = 'utf-8'
server = "vlt-k8s-master.provider.rsv.dir:9095"
Producer = kafka.KafkaProducer(bootstrap_servers=server,
                               key_serializer=lambda x: str(x).encode(encoding),
                                value_serializer=lambda x: json.dumps(x).encode(encoding))

topic = "dev_fill_redis"

# generate a random key
key = uuid4().hex
# record = {"competition":"F1",  "runuid": ["4402AC2E-14AC-4155-9EC7-5758DE3DD015"], "year": [2024]}
record = {"competition":"F1",  "runuid": ["3EFAB095-645F-45D2-86B8-9C36B835133C"], "year": [2024]}

if __name__ == "__main__":
    Producer.send(topic=topic, key=key, value=record)
    Producer.flush()