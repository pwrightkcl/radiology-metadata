# Radiology metadata

## Mapping DICOM metadata to OMOP Medical Imaging CDM

Mapping was done manually using Excel tables of DICOM strings, in order of occurrence, mapping to subsets of the OMOP concept table.

The working spreadsheets are in the [mapping](https://nhs-my.sharepoint.com/:f:/r/personal/paul_wright51_nhs_net/Documents/rad-ext/omop/mapping?csf=1&web=1&e=qaem5O) 
folder on GSTT OneDrive

Copies are stored in `/mnt/dgxstorage/radiology/omop/maps/` on preprod1

For space, this repo only contains examples of the final three CSV files used by [dicom_to_omop1.py](dicom_to_omop1.py).

### Modality

`ModalitiesInStudy.xlsx`

DICOM ModalitiesInStudy strings were split and than mapped to OMOP procedure code.

Non-image codes were set to 0.

If only CT and PT were present, the OMOP code for combined CT+PET was used.

Results were output as lists of integers (or empty lists if no matches).

### Anatomy

Anatomy was mapped as follows.

`dicom_anatomy.ods`

Targets were from the [DICOM Part 16 Table L1](https://dicom.nema.org/medical/dicom/current/output/html/part16.html#table_L-1), either the SNOMED description or the DICOM keyword (e.g. ABDOMENPELVIS). Some labels were added manually.

This table is used in Paul Nagy's DICOM to OMOP code in [transform_dicom_to_omop.ipynb](https://github.com/paulnagy/DICOM2OMOP/blob/main/dicom_to_omop/transform_dicom_to_omop.ipynb) (cell #30 and below).

`dicom_anatomy2omop.csv`

Table L1 was merged with the OMOP concept table using the SNOMED codes.

`BodyPartExamined.xlsx`

BodyPartExamined and StudyDescription were each mapped to L1. For multiple matches, the first was selected as the most descriptive one. For a mixture of granularity, e.g. "abdomen" and "kidney" the more granular was put first. For multiple regions, e.g. "HEADNECK", the concept covering all the regions was put first, followed by the individual regions, e.g. "HEADNECK, HEAD, NECK".

About 500 rows of BodyPartExamined were reviewed, matching 99.7% of studies or confirming no anatomy in the text.

`StudyDescription_anatomy.xlsx`

About 1000 rows of StudyDescription were reviewed, matching 89.2% of studies, confirming 7.3% with no anatomy in the text.

`PartDesc.xlsx`

Next, the two mappings were merged in "PartDesc", first selecting which of the primary concepts to keep as primary (if they differed) and second by including all of the secondary concepts, after removing duplicates. If one label subsumed the other, e.g. "abdomen" and "kidney" or "pelvis" and "hip", the more granular one was preferred. If the two labels were exclusive, e.g. BodyPartExamined="abdomen" and StudyDescription="MRI Pelvis", then the study description was preferred.

About 1500 rows were reviewed, matching 89.1% of studies, confirming 4.2% with no anatomic label.

### Procedure

First, the OMOP concept and concept relationship tables were used to find all unique children of the 'imaging' procedure
(4180938).

See [make_imaging_procedures.py](make_imaging_procedures.py)

`StudyDescription_procedure.xlsx`

StudyDescription strings were matched to OMOP procedure concept IDs. At the first occurrence of a general type of study, I defined a search column to sort the table to group together similar rows (e.g., "CT abdomen"), I searched for all rows matching "CT*abd" case-insensitive, e.g.:

* CT abdomen
* CT ABDOMEN
* CT abdo+C

and labelled with the cluster of concepts around that idea, e.g.:

* CT of abdomen
* CT of abdomen with contrast
* CT of abdomen and pelvis
* etc.

This is why the table starts with a solid chunk of matches, with more intermittent matches further down the table.

About 220 rows were reviewed, with similar matches bringing the total to 1947, covering about 83% of studies (86% including those confirmed as having no procedure info, e.g. "Foreign Film").
