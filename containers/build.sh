#!/bin/bash

REPOSITORY="367316522684.dkr.ecr.us-east-2.amazonaws.com/emqx_retainer"
IMAGE_NAME="emqx_retainer"
TAG=$1

if [ -z "$TAG" ]; then
    echo "You must specify the image tag: ./build.sh v0.0.1"
    exit 1
fi

rsync -rv --exclude-from=.excludersync  ../ emqx-retainer

# docker build . -t emqx_v4.0.2
docker build -t $IMAGE_NAME:"$TAG" .

# Push the image
if [ "$2" == "push" ]; then
    echo "Pushing image ...."
    docker tag $IMAGE_NAME:"$TAG" $REPOSITORY:"$TAG"
    docker push $REPOSITORY:"$TAG"
fi

rm -rf emqx-retainer
