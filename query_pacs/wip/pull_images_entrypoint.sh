#!/bin/bash
# Wrapper script for pull_images.py
# Make a usage string that advises the acceptable values of the second argument "save_pixel_data" are "true", "false" or nothing
usage="Usage: $0 <image_index> <output_dir> [<chunk_size>] [<retry_limit>]
  <image_index>: pandas dataframe in parquet format listing SOP Instance UIDs to pull.
  <output_dir> (path): directory to save logs.
  <chunk_size>: [optional] number of images to pull at once [1].
  <retry_limit>: [optional] number of times to retry failed pull requests [10]
"
script_dir=$(dirname "$(readlink -f "$0")")

py_file=${script_dir}/pull_images.py
if [[ ! -f $py_file ]]
then
  echo "Must have python script in the same directory: $py_file"
  exit 1
fi

env_script=${script_dir}/set_pacs_envs.sh
if [[ ! -f $env_script ]]
then
  echo "Must have environment script in the same directory: $env_script"
  exit 1
fi

image_index="$1"
if [[ -z "$image_index" ]]
then
  echo "$usage"
  exit 1
fi

output_dir="$2"
if [[ -z "$output_dir" ]]
then
  echo "$usage"
  exit 1
fi

if [[ ! -d "$output_dir" ]]
then
  mkdir -p "$output_dir"
fi

chunk_size_arg=""
if [[ ! -z "$3" ]]
then
  chunk_size_arg="--chunk_size $3"
fi

retry_limit_arg=""
if [[ ! -z "$4" ]]
then
  retry_limit_arg="--retry_limit $4"
fi

. "$env_script"


command="python -u ""$py_file"" --image_index ""$image_index"" --output_dir ""$output_dir"" ""$chunk_size_arg"" ""$retry_limit_arg"""
echo "Command to run:"
echo "$command"
$command |& tee "$output_dir"/pull_images.log
