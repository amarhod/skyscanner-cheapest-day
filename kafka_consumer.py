from time import sleep
from json import loads
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


TOPIC = "skyscanner_test"
SEC = 30

def main():
    conf = SparkConf().setAppName("KafkaSkyscanner").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, SEC)
    params = {
                'bootstrap.servers':'localhost:9092',
                'group.id':'skyscanner-group', 
                'fetch.message.max.bytes':'15728640',
                'auto.offset.reset':'largest'
                }

    kafka_stream = KafkaUtils.createDirectStream(ssc, [TOPIC], params)
    kafka_stream = kafka_stream.map(lambda x: loads(x[1]))
    kafka_stream.pprint()

    ssc.start()
    # Run stream for at least 10 minutes to prevent termination between message production
    sleep(600) 
    ssc.stop(stopSparkContext=True,stopGraceFully=True)

if __name__ == "__main__":
    main()