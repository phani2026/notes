#!/bin/bash

#SCRIPT TO BUILD DOCKER IMAGE


docker rmi jpy:alpine-j8-p3.6

docker build -t jpy:alpine-j8-p3.6 .

