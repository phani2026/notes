#!/bin/bash

#SCRIPT TO BUILD DOCKER IMAGE


docker rmi jpy:8-jre-slim-p-3.6.5

docker build -t jpy:8-jre-slim-p-3.6.5 .


docker run -idt -h controller \
    -e ENGINE_HOST=localhost \
    -e ENGINE_PORT=9096 \
    -p 9097:9094 \
    -p 7077:7077 \
    --name java_8 python