#!/usr/bin/env bash

# data location is wherever you want the data to be stored
data_location=/media/${USER}/Drive/minio
mkdir -p data_location

docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --user $(id -u):$(id -g) \
   --name minio \
   -e "MINIO_ROOT_USER=admin" \
   -e "MINIO_ROOT_PASSWORD=password" \
   -v $data_location:/data \
   minio/minio:latest server /data --console-address ":9001"