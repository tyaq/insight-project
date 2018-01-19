import random
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import json

class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, source_symbol):
        seed = random.seed(a=42) #subset of the total volume of discogs releases w/ complete metadata
        msg_cnt = 0
        user_counter=1
        while True:
            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
            device_id='device'+str(user_counter) #device IDs will be in the form userXXXX
            temp = random.randint(-20,10)
            kws = random.randint(200,600)/3600
            message_info = json.dumps({ "time":time_field,
                                          "device_id":device_id,
                                          "temp":temp,
                                          "kws":kws })
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