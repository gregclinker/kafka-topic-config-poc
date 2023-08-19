echo $KAFKA
/opensource/kafka_2.13-3.1.0/bin/kafka-console-consumer.sh --bootstrap-server=$KAFKA:29092 \
  --timeout-ms=5000 \
  --property print.offset=true \
  --property print.timestamp=true \
  --topic=pubsub-topic \
  --from-beginning