# Radiology metadata

A collection of tools to extract, transform, and otherwise curate radiology metadata.

Clinical radiology data is messy! It is difficult to map it perfectly on to research-grade semantics. These scripts use a heuristic approach with a Pareto-informed philosophy: start with the 80% of images you can process with 20% of your effort, and leave the 20% of images that take 80% of your effort till last.

These tools are focused on working at scale. As such they tend to produce a lot of logs for monitoring progress and performance, and they implement chunking to avoid memory pressure during processing and to allow interrupted processes to be resumed.

Most of the code was used originally written in the [London AI Centre](https://github.com/londonaicentre/rad-ext/) to extract DICOM metadata and transform into OMOP's Medical Imaging Common Data Model. The BIDS section was used extensively to generate datasets for the Wellcome High-Dimensional Neurology project.

* To run this code, you can first to build a [docker](docker) image to provide the necessary environment.
* To [query pacs](query_pacs), you must run the code from a machine with a connection to PACS.
* If you have a set of PACS query responses in DICOM format, or if you are starting with a dataset of actual DICOM images files, you can [index](index_dicom) these to a Pandas DataFrame.
* In [explore](explore) you can find a example scripts that summarise the DICOM index to human-readable Excel.
* The [MI-CDM](MI-CDM) folder contains code to map DICOM index metadata to the OMOP Medical Imaging CDM.
* The [BIDS](BIDS) can take you from a DICOM index to NIfTI images organised according to the [Brain Imaging Data Structure](https://bids-specification.readthedocs.io/).*
