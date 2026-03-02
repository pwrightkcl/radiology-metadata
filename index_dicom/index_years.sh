#!/bin/bash
# Index query results by year
chunk_size=20000
script_dir=$(dirname "$(readlink -f "$0")")
py_file=${script_dir}/index_dicom_files.py
if [[ ! -f $py_file ]]
then
  echo "Must have python script in the same directory: $py_file"
  exit 1
fi

fields_file=${script_dir}/study_fields.txt
if [[ ! -f $fields_file ]]
then
  echo "Must have fields file in the same directory: $fields_file"
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

# The data layout should be:
# in:
# ./data/responses/studies/2023/
# etc.
# out:
# ./data/indices/studies/2023/

# make sure dicom_dir string contains "/responses/"
if [[ $dicom_dir != */responses/* ]]
then
  echo "Error: $dicom_dir does not contain '/responses/'."
  exit 1
fi
index_dir=${dicom_dir/\/responses\//\/indices\/}

for year in $years  # deliberately not quoting $years
do
  in_dir=$dicom_dir/$year
  out_dir=$index_dir/$year
  mkdir -p "$out_dir"
  echo -n "Starting ${year}: " && date
  python -u "$py_file" --in_dir "$in_dir" --out_dir "$out_dir" --fields_file "$fields_file" --chunk_size $chunk_size \
    | tee -a "${out_dir}/index_dicom_files.log"
  echo -n "Finished ${year}: " && date
done
echo "All years indexed."
