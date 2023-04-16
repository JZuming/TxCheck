#!/bin/sh -v

IMAGE_NAME=txcheck-mariadb
CONTAINER_NAME=txcheck-mariadb-container
TOOL_NAME=TxCheck

rm $TOOL_NAME -rf
cp ../$TOOL_NAME . -r

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

docker build -t $IMAGE_NAME .
docker run -itd --name $CONTAINER_NAME $IMAGE_NAME

rm $TOOL_NAME -rf
