from time import sleep
from json import loads
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster

TOPIC = "skyscanner_test"
KEYSPACE = "skyscanner_test"
SEC = 30
def weighted_average(new_price, old_price, count):
    return (float((count * old_price) + new_price)/(count + 1))

def save_to_cassandra(session, rdd):
    rdd = rdd.collect()
    for message in rdd:
        days_to_departure = message['days_to_departure']
        origin = message['origin']
        destination = message['destination']
        min_price = message['min_price']
        unique_count = message['unique_count']
        cached_timestamp = message['cached_timestamp']
        query_date = message['query_date']
        query = session.execute(
        """
        SELECT * FROM quotes WHERE query_date=%s AND
                    days_to_departure=%s AND
                    origin=%s AND
                    destination=%s
        """,
        (message['query_date'],message['days_to_departure'],message['origin'],message['destination']))
        for row in query:
            old_min_price = row.min_price
            old_unique_count = row.unique_count
            old_cached_timestamp = row.cached_timestamp 
            if(cached_timestamp != old_cached_timestamp):
                print("Updating existing row")
                min_price = weighted_average(min_price, old_min_price, old_unique_count)
                unique_count = old_unique_count + 1
        session.execute(
        """
        INSERT INTO quotes (query_date,
                    days_to_departure,
                    origin,
                    destination,
                    min_price,
                    unique_count,
                    cached_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (query_date, days_to_departure, origin, destination, min_price, unique_count, cached_timestamp)
    )

def main():
    cluster = Cluster()
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)
    session.set_keyspace(KEYSPACE)
    session.execute("""
            CREATE TABLE IF NOT EXISTS quotes (
                query_date text,
                days_to_departure int,
                origin text,
                destination text,
                min_price float,
                unique_count int,
                cached_timestamp text,
                PRIMARY KEY ((query_date, days_to_departure, origin, destination))
            )
            """)

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
    try:
        kafka_stream.foreachRDD(lambda x: save_to_cassandra(session, x))
    except:
        print("Failed to save to cassandra")
    ssc.start()
    # Run stream for at least 10 minutes to prevent termination between message production
    sleep(600)
    ssc.stop(stopSparkContext=True,stopGraceFully=True)


if __name__ == "__main__":
    main()
