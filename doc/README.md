# Radiology metadata

## Miscellaneous notes

### DICOM notes

Attributes with VR of PN may be searched [case insensitively](https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.2.2.2.html).

Pydicom gives warnings of `Invalid value for VR UI` for some values in the `StudyInstanceUID` field. This is because the
UID contains parts with leading zeros, which are not allowed. The warning gives a link, which then leads to an appendix
explaining this. Online discussion raises the dilemma of correcting the UID, which would then prevent it matching its
original value, if compared as a string.
