# skyscanner-lowest-price
Analysis of the best time point to buy tickets on Skyscanner. By utilizing Kafka, a live stream is simulated which contains price and flight information on a number of routes. With the help of Spark Streaming and Cassandra, the time point (days before departure) of the lowest price across all flights is calculated. 
