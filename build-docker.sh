#!/bin/bash

docker build -t amplab/ray:devel docker/devel
docker build -t amplab/ray:deploy docker/deploy
