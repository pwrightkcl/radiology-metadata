# Radiology metadata

## Index DICOM metadata

These scripts will read metadata from a directory full of DICOM (`.dcm`) files and collect specified attributes into a Pandas DataFrame, saved as CSV and parquet (or pickle if saving as parquet raises an exception). For DICOM response files returned by `C-FIND` query (e.g. from DCMTK `findscu`) use `index_dicom_files.py` to iterate over every file. For collections of image files, use `index_dicom_dirs.py` to process only the first file in each directory, assuming each directory contains instances from the same series (e.g. slices of a CT can) and you are not interested in attributed that vary between instances.

Arguments:

* `--in_dir`: the directory containing DICOM files
* `--out_dir`: the directory where the CSV and parquet files will be saved
* `--chunk_size` [optional]: save the DataFrame after processing the specified number of files
  * This makes it easier to resume if indexing is interrupted.
  * It also avoids the script slowing down as the DataFrame grows.
* `--fields_file` [optional]: a list of DICOM keywords to index
  * If left blank:
    * `index_dicom_files.py` will try to index every field it finds (assuming a query response will not have many fields)
    * `index_dicom_dirs.py` will default to a small set of generic fields, assuming you are indexing actual image files with thousands of fields  
  * Examples:
    * `dicom_fields_files_[study|series|instance].txt`: for query responses at the study, series, and instance level
    * `dicom_fields_dirs.txt`: for image files containing a mixture of CT and MR modalities

As with [querying PACS](../query_pacs), the scripts are most conveniently run with a bash wrapper. The `index_years.sh` script is a wrapper corresponding to the `query_studies_by_year.sh` script.

> [!NOTE]
> The latest [query_pacs](../query_pacs/) scripts write results directly to data tables so these scripts are no longer needed for that purpose. They are still useful when dealing with actual images, or queries using DCMTK, which saves responses to .dcm files.
