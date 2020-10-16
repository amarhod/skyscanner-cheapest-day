from time import sleep
from json import dumps
from kafka import KafkaProducer
from skyscanner_consumer import get_route_messages


def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    while(True):
        list_of_messages = get_route_messages()
        for message in list_of_messages:
            producer.send('skyscanner_test', value=message)
            sleep(3)
        print(f"Produced {len(list_of_messages)} messages to Kafka topic: skyscanner_test")
        print("sleeping...")
        sleep(60*1)
        print("waking up")

    
if __name__ == "__main__":
    main()