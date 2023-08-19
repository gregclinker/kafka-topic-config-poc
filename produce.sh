echo "/opensource/kafka_2.13-3.1.0/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=$KAFKA:29092 --num-records=10 --record-size=200 --throughput=-1 --topic=pubsub-topic"
/opensource/kafka_2.13-3.1.0/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=$KAFKA:29092 --num-records=10 --record-size=200 --throughput=-1 --topic=pubsub-topic
