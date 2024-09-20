import kafka
import json

encoding = 'utf-8'
server = "vlt-k8s-master.provider.rsv.dir:9095"
Producer = kafka.KafkaProducer(bootstrap_servers=server,
                                value_serializer=lambda x: json.dumps(x).encode(encoding),
                                key_serializer=lambda x: str(x).encode(encoding))


topic = "dev_fill_redis"

# generate a random key
import random
import string
key = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
record = {"competition":"F1",  "ruids": [1, 2, 3], "years": [2019, 2020, 2021]}
value = record

if __name__ == "__main__":
    Producer.send(topic=topic, key=key, value=value)
    Producer.flush() 