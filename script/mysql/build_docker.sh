#!/bin/sh -v

IMAGE_NAME=txcheck-mysql
CONTAINER_NAME=txcheck-mysql-container

docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

docker build -t $IMAGE_NAME .
docker run -itd --name $CONTAINER_NAME $IMAGE_NAME

