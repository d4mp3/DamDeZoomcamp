import json
import time
import pandas as pd

from kafka import KafkaProducer

from settings import INPUT_DATA_PATH, BOOTSTRAP_SERVERS


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()


#test-topic with test data
# t0 = time.time()
# topic_name = 'test-topic'

# for i in range(10):
#     message = {'number': i}
#     producer.send(topic_name, value=message)
#     print(f"Sent: {message}")
#     time.sleep(0.05)

# producer.flush()

# t1 = time.time()
# print(f'took {(t1 - t0):.2f} seconds')

t0 = time.time()
topic_name = 'green-topic'
df_green = pd.read_csv(INPUT_DATA_PATH, usecols=[
                                            'lpep_pickup_datetime', 
                                            'lpep_dropoff_datetime', 
                                            'PULocationID', 
                                            'DOLocationID',
                                            'passenger_count',
                                            'trip_distance',
                                            'tip_amount'
                                        ])


for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)

producer.flush()
t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')