import argparse
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.misc import is_dicom

LOGGER = logging.getLogger(__name__)


def looks_like_dicom(path: Path) -> bool:
    """Return True if a file appears to be a readable DICOM object."""
    if is_dicom(path):
        return True

    try:
        ds = pydicom.dcmread(path, stop_before_pixels=True, force=True)  # pyright: ignore[reportUnknownMemberType]
    except (InvalidDicomError, OSError):
        return False

    required_tags = ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID")
    return all(hasattr(ds, tag) for tag in required_tags)


def main(input_dir: str, output_dir: str):
    """Copy DICOM files in input_dir to a new structure in output_dir
    
    Args:
        input_dir (str): Path to the input directory containing DICOM files.
        output_dir (str): Path to the output directory where copied files will be saved.
    
    Outputs:
        DICOM files copied and structured as follows:
        output_dir/
            StudyDate/
                StudyInstanceUID/
                    SeriesInstanceUID/
                        SOPInstanceUID.dcm
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Check output_path is empty
    if any(output_path.iterdir()):
        raise ValueError(f"Output directory {output_path} is not empty. Please provide an empty directory.")

    run_timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    log_path = output_path / f"rename_dicom_{run_timestamp}.log"
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(file_handler)
    LOGGER.info("Writing log file to %s", log_path)

    total_files = 0
    non_dicom_files = 0
    dicom_files = 0
    copied_files = 0
    failed_files = 0

    LOGGER.info("Scanning input directory: %s", input_path)

    for dicom_file in input_path.rglob("*"):
        if not dicom_file.is_file():
            continue

        total_files += 1

        if not looks_like_dicom(dicom_file):
            non_dicom_files += 1
            continue

        dicom_files += 1

        try:
            ds = pydicom.dcmread(dicom_file)  # pyright: ignore[reportUnknownMemberType]
            study_date = ds.StudyDate
            study_instance_uid = ds.StudyInstanceUID
            series_instance_uid = ds.SeriesInstanceUID
            sop_instance_uid = ds.SOPInstanceUID

            new_dir = output_path / study_date / study_instance_uid / series_instance_uid
            new_dir.mkdir(parents=True, exist_ok=True)

            new_file_path = new_dir / f"{sop_instance_uid}.dcm"
            shutil.copy2(dicom_file, new_file_path)
            copied_files += 1
            LOGGER.debug("Copied %s -> %s", dicom_file, new_file_path)
        except (InvalidDicomError, AttributeError, KeyError, OSError, ValueError) as error:
            failed_files += 1
            LOGGER.warning("Error processing %s: %s", dicom_file, error)

    LOGGER.info(
        "Done. total=%d dicom=%d copied=%d non_dicom=%d failed=%d",
        total_files,
        dicom_files,
        copied_files,
        non_dicom_files,
        failed_files,
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Copy DICOM files into a metadata-based folder structure.")
    parser.add_argument(
        '--input_dir',
        required=True,
        help='Path to source directory containing DICOM and non-DICOM files.'
    )
    parser.add_argument(
        '--output_dir',
        required=True,
        help='Path to empty output directory where copied DICOM files are written.'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging verbosity (default: INFO).'
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(message)s',
    )
    main(args.input_dir, args.output_dir)

