

from confluent_kafka import Producer
import csv, json


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

p = Producer({'bootstrap.servers': 'localhost:9'})

topic_name = 'topic-one'
csv_path = '/Users/phaneendra/Downloads/demographics_1M.csv'

with open(csv_path) as file:
    reader = csv.DictReader(file, delimiter=",")
    for row in reader:
        data = {}
        data['cust_id'] = row['cust_id']
        data['name'] = row['name']
        data['gender'] = row['gender']
        data['income'] = row['income']
        data['pin'] = row['pin']
        p.produce(topic_name, json.dumps(data).encode("utf-8"), callback=delivery_report)
        p.flush()