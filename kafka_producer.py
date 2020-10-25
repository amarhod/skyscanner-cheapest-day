from time import sleep
from json import dumps
from kafka import KafkaProducer
from skyscanner_consumer import get_route_messages

MIN = 1

def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    while(True):
        routes_this_month = get_route_messages()
        routes_next_month = get_route_messages(1)
        list_of_messages = routes_this_month + routes_next_month
        for message in list_of_messages:
            producer.send('skyscanner_test', value=message)
        print(f"Produced {len(list_of_messages)} messages to Kafka topic: skyscanner_test")
        print("sleeping...")
        sleep(60*MIN)
        print("waking up")

    
if __name__ == "__main__":
    main()