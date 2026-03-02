# Radiology metadata

## Summarise study metadata

These scripts will create summaries of DICOM dataframes created using [index_dicom](../index_dicom/). They first count the number of missing or empty values and then count the occurrences of each value of the fields or groups of fields specified. They save the summaries to Excel.

The script `summarise_study_metadata.py` also breaks down the "ModalitiesInStudy" field into a count of each modality.
