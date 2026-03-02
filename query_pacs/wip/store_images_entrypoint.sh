#!/bin/bash
# Wrapper script for store_images.py
# Make a usage string that advises the acceptable values of the second argument "save_pixel_data" are "true", "false" or nothing
usage="Usage: $0 <output_dir> [<save_pixel_data>]
  <output_dir> (path): directory to store the DICOM files.
  <save_pixel_data> (true/false): [optional] save DICOM pixel data (default: false)."
script_dir=$(dirname "$(readlink -f "$0")")

py_file="$script_dir"/store_images.py
if [[ ! -f "$py_file" ]]
then
  echo "Must have python script in the same directory: $py_file"
  exit 1
fi

env_script="$script_dir"/set_pacs_envs.sh
if [[ ! -f "$env_script" ]]
then
  echo "Must have environment script in the same directory: $env_script"
  exit 1
fi

output_dir="$1"
if [[ -z "$output_dir" ]]
then
  echo "$usage"
  exit 1
fi

if [[ ! -d "$output_dir" ]]
then
  mkdir -p "$output_dir"
fi

save_pixel_arg=""
if [[ ! -z "$2" ]]
then
  if [ "$2" = true ]
  then
    save_pixel_arg="--save_pixel_data"
  elif [ "$2" != false ]
  then
    echo "$usage"
    exit 1
  fi
fi

. "$env_script"

python -u "$py_file" --output_dir "$output_dir" $save_pixel_arg |& tee "$output_dir"/store_images.log
