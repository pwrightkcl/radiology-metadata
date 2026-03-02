#!/bin/bash
# Query PACS for study-level metadata by year
# Usage:
#   query_studies_by_year.sh $dicom_dir $years
#
#   Arguments:
#     dicom_dir: root directory under which subdirectories will be created for each year. Must exist.
#     years: quoted string of years separated by white space, will be split later

script_dir=$(dirname "$(readlink -f "$0")")

query_script=${script_dir}/query.py

if [[ ! -f $query_script ]]
then
  echo "Must have python script in the same directory: $query_script"
  exit 1
fi

dicom_dir="$1"
years="$2"
if [[ -z $dicom_dir || -z $years ]]
then
  echo "Usage: $0 <dicom_dir> <years>"
  exit 1
fi
if [[ ! -d $dicom_dir ]]
then
  echo "Error: $dicom_dir is not a directory."
  exit 1
fi

for year in $years  # do not quote years so the strings will be split
do
  output_dir="$dicom_dir/$year"
  mkdir -p "$output_dir"
  echo "Querying PACS for $year"
  echo -n "Start: " && date
  uv run "$query_script" by_date --start_date "$year-01-01" --end_date "$year-12-31" --output_dir "$output_dir"
  echo -n "End: " && date
done
echo "All years queried."
