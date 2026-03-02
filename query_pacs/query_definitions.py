#!python
import argparse
from pathlib import Path

from pydicom import Dataset, Sequence
from pydicom.dataset import FileMetaDataset
from pydicom.uid import UID, ExplicitVRLittleEndian, generate_uid, PYDICOM_IMPLEMENTATION_UID, PYDICOM_ROOT_UID


def make_study_query():
    """Construct a pydicom Dataset defining the default study query.

    Returns:
        Dataset: A pydicom Dataset object with the study query parameters.
    """
    study_query = Dataset()

    study_query.QueryRetrieveLevel = 'STUDY'
    study_query.AccessionNumber = ''
    study_query.ModalitiesInStudy = ''
    study_query.StudyDescription = ''
    study_query.PatientID = ''
    study_query.IssuerOfPatientID = ''
    study_query.TypeOfPatientID = ''
    study_query.RETIRED_OtherPatientIDs = ''

    PatientIDsDataset = Dataset()
    PatientIDsDataset.PatientID = ''
    PatientIDsDataset.IssuerOfPatientID = ''
    PatientIDsDataset.TypeOfPatientID = ''
    study_query.OtherPatientIDsSequence = Sequence([PatientIDsDataset])

    study_query.BodyPartExamined = ''
    study_query.StudyInstanceUID = ''
    study_query.NumberOfStudyRelatedSeries = None
    study_query.NumberOfStudyRelatedInstances = None

    return study_query


def make_series_query():
    """Construct a pydicom Dataset defining the default series query.

    Returns:
        Dataset: A pydicom Dataset object with the series query parameters.
    """
    series_query = Dataset()
    series_query.QueryRetrieveLevel = 'SERIES'
    query_attributes = {'StudyInstanceUID': '', 'AccessionNumber': '', 'StudyDescription': '', 'StudyDate': '',
                        'StudyTime': '', 'BodyPartExamined': '',
                        'SeriesInstanceUID': '', 'Modality': '', 'SeriesDescription': '', 'SeriesNumber': '',
                        'SeriesDate': '', 'SeriesTime': '', 'NumberOfSeriesRelatedInstances': '',}
    for attribute, default in query_attributes.items():
        setattr(series_query, attribute, default)

    return series_query


def make_image_query():
    """Construct a pydicom Dataset defining the default image query.

    Returns:
        Dataset: A pydicom Dataset object with the image query parameters.
    """
    image_query = Dataset()
    image_query.QueryRetrieveLevel = 'IMAGE'
    query_attributes = {
        # Study
        'StudyDate': '', 'StudyTime': '', 'AccessionNumber': '', 'StudyDescription': '', 'StudyInstanceUID': '',
        # Series
        'SeriesDate': '', 'SeriesTime': '', 'Modality': '', 'SeriesDescription': '', 'BodyPartExamined': '',
        'ProtocolName': '', 'SeriesInstanceUID': '', 'SeriesNumber': '',
        # General Image
        'ImageType': '','InstanceNumber': None, 'ScanOptions': '',
        # CR / CT Image
        'KVP': None, 'ExposureTime': None,
        # MR Image
        'ScanningSequence': '', 'RepetitionTime': None, 'EchoTime': None, 'FlipAngle': None,
        # Image Plane
        'SliceThickness': None,
        # Image Pixel
        'Rows': None, 'Columns': None,
        # Contrast/Bolus
        'ContrastBolusAgent': '',
        # SOP Common
        'InstanceCreationDate': '', 'InstanceCreationTime': '', 'SOPInstanceUID': '', }
    for attribute, default in query_attributes.items():
        setattr(image_query, attribute, default)

    return image_query


def save_query(query: Dataset, filename: Path):
    """Add required metadata to the study query and save it to a DICOM file.

    Parameters:
        query (Dataset): The pydicom Dataset object to save.
        filename (Path): The filename to save the DICOM file as.

    Returns:
        None

    Outputs:
        A DICOM file with the study query and required metadata.
    """

    # Populate required values for file meta information
    # https://pydicom.github.io/pydicom/dev/auto_examples/input_output/plot_write_dicom.html
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = UID(PYDICOM_ROOT_UID + '1.2.3')  # An arbitrary private class
    file_meta.MediaStorageSOPInstanceUID = generate_uid(PYDICOM_ROOT_UID)  # Unique UID based on pydicom root
    file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    query.file_meta = file_meta
    query.save_as(filename, enforce_file_format=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create base DICOM queries.')
    parser.add_argument('output_dir', type=str, help='Directory to save the base queries')
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    my_study_query = make_study_query()
    save_query(my_study_query, output_dir / 'study_query.dcm')

    my_series_query = make_series_query()
    save_query(my_series_query, output_dir / 'series_query.dcm')

    my_image_query = make_image_query()
    save_query(my_image_query, output_dir / 'image_query.dcm')
