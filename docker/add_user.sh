#!/bin/bash

# Check that the user has provided exactly two inputs, first the base image and second the tag for the built image. If not, print the usage and exit.
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <base_image> <docker_tag>"
  exit 1
fi

base_image="$1"
docker_tag="$2"

# Set user and group variables outside of sudo
my_user_id="$(id -u)"
my_group_id="$(id -g)"
my_user="${USER}"

# Build the image by calling on your Dockerfile (named Dockerfile in this instance) and passing
# the various build arguments
docker build . -f Dockerfile_add_user \
 --network=host \
 --tag "${docker_tag}" \
 --build-arg BASE_IMAGE="$base_image" --build-arg USER_ID="$my_user_id" --build-arg GROUP_ID="$my_group_id" --build-arg USER="$my_user" \
# --no-cache
