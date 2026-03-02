#!/bin/bash
# Query PACS for months of series data based on saved study indices
# Requires study indices saved by year and month

script_dir=$(dirname "$(readlink -f "$0")")

query_script=${script_dir}/query.py

if [[ ! -f $query_script ]]
then
  echo "Must have python script in the same directory: $query_script"
  exit 1
fi

mkdir -p "/home/pwright/data/responses/series"

for yearmonth in 2025{01..12}
do
  echo "######"
  echo "$yearmonth"
  echo "######"
  echo ""
  uv run "$query_script" by_study \
    --study_index "/home/pwright/data/indices/studies/radiology_gstt_studies_${yearmonth}.parquet" \
    --output_dir "/home/pwright/data/responses/series/radiology_gstt_series_${yearmonth}" \
    --query_level "series" \
    --chunk_size 1000 \
    --min_studies_per_chunk 9
done
