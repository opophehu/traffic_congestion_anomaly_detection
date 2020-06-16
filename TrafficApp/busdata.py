import json
import requests
from datetime import datetime
import uuid
from pytz import timezone
import pytz
import time
from pykafka import KafkaClient
from pred import get_dur_pred


def generate_uuid():
    return uuid.uuid4()

def get_time_now():
    date_format='%Y-%m-%d %H:%M:%S'
    date = datetime.now(tz=pytz.utc)
    print ('Current date & time is:', date.strftime(date_format))

#     date = date.astimezone(timezone('US/Pacific'))

#     print ('Local date & time is  :', date.strftime(date_format))
    return str(date.strftime(date_format))

def get_buses():
    buses = []
    for i in kc_bus.json()['entity']:
        k = i['vehicle']['vehicle']['id']
#         print (k)
        buses.append(str(k))
    buses = buses[:3]
    print (buses)
    return buses

# kafka producer
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_final123']
producer = topic.get_sync_producer()

# Seattle real time Bus Data
data = {}
kc_bus_url = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions_pb.json"
kc_bus = requests.request("get", kc_bus_url)

def generate_checkpoint():
    while True:
        kc_bus = requests.request("get", kc_bus_url)
        buses = get_buses()
        for busline in buses:
            data['busline'] = busline
            for i in kc_bus.json()['entity']:
                if i['vehicle']['vehicle']['id'] == busline:
                    data['key'] = data['busline'] + "_" + str(generate_uuid())
                    data['timestamp'] = str(get_time_now())
                    data['latitude'] = i['vehicle']['position']['latitude']
                    data['longitude'] = i['vehicle']['position']['longitude']
                    data['status'], data['msg'] = get_dur_pred(data['latitude'], data['longitude'], data['timestamp'])
                    message=json.dumps(data)
                    print(message)
                    producer.produce(message.encode('ascii'))
                    time.sleep(2)

generate_checkpoint()