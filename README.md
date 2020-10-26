## Skyscanner cheapest day
Analyzes the best time point to buy flight tickets on Skyscanner. Developed in collaboration with [@EleonoraBorzis](https://github.com/EleonoraBorzis) for a course project. 

## Code functionality 
- **skyscanner_consumer** - Queries the Skyscanner Flight Search API for a number of routes and receives the minimum price for each route across multiple days. Useful information (e.g. cached timestamp, minimum price, origin, destination etc) for each route and date gets stored in a list.
- **kafka producer** - Calls skyscanner_consumer and produces each message in the list to a Kafka topic.
- **kafka_consumer** - Consumes each message from the Kafka topic and UPSERTs it into a Cassandra table.
- **analyzer** - Calculates the best day before departure, to buy a ticket, for a given range (e.g. 1-30 days). By querying the Cassandra table and calculating the best day for each route, the day that occures most frequent across all routes is found and printed.
  
## How to run
How to run in the terminal:
```
1. Start Zookeeper
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

1. Start Kafka
kafka-server-start.sh $KAFKA_HOME/config/server.properties

1. Create a topic named skyscanner_test
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic skyscanner_test

4. Start Cassandra 
cassandra -f

5. Create a virtualenv and install packages with requirements.txt
pip3 install virtualenv
virtualenv skyscanner
source skyscanner/bin/activate
pip3 install -r requirements.txt

6. Submit Spark job
$SPARK_HOME/bin/spark-submit --jars /.../spark-streaming-kafka-0-8-assembly_2.11-2.4.3.jar kafka_consumer.py

7. Start Kafka producer
source skyscanner/bin/activate
python3 kafka_producer.py

8. Start analyzer
source skyscanner/bin/activate
python3 analyzer.py
```