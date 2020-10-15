from time import sleep
from json import dumps
from kafka import KafkaProducer

def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    for e in range(1000):
        data = {'number' : e}
        producer.send('numtest', value=data)
        sleep(5)

if __name__ == "__main__":
    main()