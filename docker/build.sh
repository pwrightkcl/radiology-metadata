#!/bin/bash

# Create a "tag" or name for the image
docker_tag="$1"
if [[ -z "$docker_tag" ]]; then
  echo "Docker tag not specified."
  exit 1
fi

# Build the image by calling on your Dockerfile (named Dockerfile in this instance) and passing
# the various build arguments
docker build -f Dockerfile -t "${docker_tag}" --network=host --no-cache ..
