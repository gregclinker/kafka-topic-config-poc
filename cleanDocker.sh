echo "docker system prune -f ; docker network prune -f ; docker volume prune -f ; docker rm -f -v $(docker ps -q -a)"
docker system prune -f ; docker network prune -f ; docker volume prune -f ; docker rm -f -v $(docker ps -q -a)