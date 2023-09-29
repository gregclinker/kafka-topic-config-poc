#
/opensource/kafka_2.13-3.1.0/bin/kafka-topics.sh --bootstrap-server=localhost:29092 --create --if-not-exists \
  --topic greg-test1 \
  --partitions 2 \
  --replication-factor 1 \
  --config min.insync.replicas=2
