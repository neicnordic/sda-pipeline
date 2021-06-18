#!/bin/bash

# Build containers
docker build -t neicnordic/sda-pipeline:latest . || exit 1


cd dev_utils || exit 1

tostart="mq db"

if [ "$STORAGETYPE" = s3 ]; then
   tostart="mq db s3"
fi

docker-compose -f compose-backend.yml up -d $tostart

RETRY_TIMES=0
for p in $tostart; do
    until docker ps -f name=$p --format {{.Status}} | grep "(healthy)"
    do echo "waiting for $p to become ready"
       RETRY_TIMES=$((RETRY_TIMES+1));
       if [ "$RETRY_TIMES" -eq 30 ]; then
	   # Time out
      docker logs $p
	   exit 1;
       fi
       sleep 10
    done
done

docker-compose -f compose-sda.yml up -d

RETRY_TIMES=0
for p in ingest verify finalize mapper intercept; do
    until docker ps -f name=$p --format {{.Status}} | grep "Up"
    do echo "waiting for $p to become ready"
       RETRY_TIMES=$((RETRY_TIMES+1));
       if [ "$RETRY_TIMES" -eq 30 ]; then
      # Time out
      docker logs $p
      docker logs mq
      exit 1;
       fi
       sleep 10
    done
done


# Show running containers
docker ps -a

