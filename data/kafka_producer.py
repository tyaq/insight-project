import random
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import json

EPOCH = datetime.utcfromtimestamp(0)

class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def unix_time_mills(self, time):
        return (time - EPOCH).total_seconds() * 1000.0

    def produce_msgs(self, source_symbol):
        seed = random.seed(a=42) #subset of the total volume of discogs releases w/ complete metadata
        msg_cnt = 0
        user_counter = 1
        while True:
            time_field = self.unix_time_mills(datetime.now())
            device_id='device'+str(user_counter) #device IDs
            temp = random.uniform(-20,10)
            kws = random.uniform(200,600)/3600
            message_info = json.dumps({ "time":time_field,
                                        "device-id":device_id,
                                        "sensor-name-1":"temp",
                                        "sensor-value-1":temp,
                                        "sensor-name-2":"kws",
                                        "sensor-value-2":kws})
            print message_info
            self.producer.send('device_activity_stream', message_info)
            msg_cnt += 1
            user_counter += 1
            if user_counter%10==0:
                user_counter=1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 