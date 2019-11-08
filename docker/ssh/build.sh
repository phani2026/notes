#!/bin/bash

#SCRIPT TO BUILD DOCKER IMAGE

docker rm --force ssh-container
docker rm --force ssh-container2
docker rmi ssh-test

docker build -t ssh-test .


#docker network create ssh-test-net


docker run -idt -h controller --net=ssh-test-net \
    -p 13122:22 \
    --name ssh-container ssh-test


docker run -idt -h controller2 --net=ssh-test-net \
    -p 13123:22 \
    --name ssh-container2 ssh-test