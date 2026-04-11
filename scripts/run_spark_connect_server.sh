#!/bin/bash
docker run -d -p 15002:15002 \
    -v $1/spark.crt:/opt/ssl/spark.crt \
    --name spark-delta \
    franciscoabsampaio/spark-connect-server:delta