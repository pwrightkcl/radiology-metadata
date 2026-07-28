import argparse
import csv
from datetime import datetime
from pathlib import Path

# import pandas as pd
import pydicom


def main(input_dir: str):
    """Set ReferringPhysicianName to "1946" in DICOM files from Insight46"""
    print(f"Setting ReferringPhysicianName to '1946' in {input_dir}")
    input_path = Path(input_dir)
    dcmfiles  = list(input_path.rglob("*.dcm"))
    for dcm in dcmfiles:
        try:
            ds = pydicom.dcmread(dcm)
            print(f"{dcm.name}: '{ds.ReferringPhysicianName}' -> '1946'")
            ds.ReferringPhysicianName = '1946'
            ds.save_as(dcm)
        except Exception as e:
            print(f"Failed to read DICOM file {dcm}: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fix ReferringPhysicianName in DICOM files.")
    parser.add_argument(
        '--input_dir',
        required=True,
        help='Path to source directory containing DICOM files.'
    )
    args = parser.parse_args()

    main(args.input_dir)