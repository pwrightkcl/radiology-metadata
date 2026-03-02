# Radiology metadata

## Overview

This code was used originally written in the [London AI Centre](https://github.com/londonaicentre/rad-ext/) to extract DICOM metadata and transform into OMOP's Medical Imaging Common Data Model.

To use this code, you will first need to build a [docker](docker) image to provide the necessary environment.

To [query pacs](query_pacs), you must run the code from a machine with a connection to PACS.

If you have a set of PACS query responses in DICOM format, or if you are starting with a dataset of actual DICOM images files, you can [index](index_dicom) these to a Pandas DataFrame.

In [explore](explore) you can find an example script that summarises the metadata DataFrame to Excel.

The [MI-CDM](MI-CDM) folder contains code to map DICOM metadata to the OMOP Medical Imaging CDM.