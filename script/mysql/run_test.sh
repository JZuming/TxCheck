#!/bin/sh

IMAGE_NAME=txcheck-mysql
CONTAINER_NAME=txcheck-mysql-container

if [ $# -lt 1 ]
then
    echo "You should input one argument (docker container number)"
    exit 1
fi
echo "Docker container number: "$1

echo "Stop the previous container ..."
docker stop $(docker ps -a --format "table {{.Names}}" | grep $CONTAINER_NAME)
echo "Remove the previous container ..."
docker rm $(docker ps -a --format "table {{.Names}}" | grep $CONTAINER_NAME)

n=1
while [ $n -le $1 ]
do
    echo "Set up container "$n" ..."
    docker run -itd -m 32g --name $CONTAINER_NAME-$n $IMAGE_NAME
    docker cp test.sh $CONTAINER_NAME-$n:/home/mysql
    docker exec -it $CONTAINER_NAME-$n nohup sh test.sh
    n=$(( $n + 1 ))
done

