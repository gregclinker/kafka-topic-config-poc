echo "docker-compose up -d"
docker-compose up -d
#
echo "docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka  | awk '{print "export KAFKA="$1}' > setKafka.sh"
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka  | awk '{print "export KAFKA="$1}' > setKafka.sh
#
echo $KAFKA | awk '{print $1,"kafka"}'
