import kafka
import time
import random

server = "vlt-k8s-master.provider.rsv.dir:9095"
topic = "dev_fill_redis"
group_id = "test_0"


Consumer = kafka.KafkaConsumer(topic, 
                bootstrap_servers=server, 
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset='earliest',
                key_deserializer=lambda x: x.decode('utf-8'),
                value_deserializer=lambda x: x.decode('utf-8'))

while True:
    results = Consumer.poll()
    if not results: pass
    
    for _, msg_batch in results.items():
        for msg in msg_batch:
            is_ok = random.random() > 0.5
            if is_ok:
                print("Done for key:", msg.key, "- data :", msg.value)
                Consumer.commit()
            else:
                print("Error for key:", msg.key, "Offset:", msg.offset)
                partition = kafka.TopicPartition(msg.topic, msg.partition)
                Consumer.seek(partition, msg.offset)

    time.sleep(2)