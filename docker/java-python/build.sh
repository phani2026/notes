#!/bin/bash

#SCRIPT TO BUILD DOCKER IMAGE


docker rmi jpy:8-jre-slim-p-3.6.5

docker build -t jpy:8-jre-slim-p-3.6.5 .

