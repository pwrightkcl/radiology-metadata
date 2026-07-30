import argparse
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
import pydicom

ATTRIBUTES = [
    'StudyDate',
    'ReferringPhysicianName',
    'PatientName',
    'PatientID',
    'StudyDescription',
    'Modality',
    'SeriesNumber',
    'SeriesDescription',
]


def main(input_dir: str):
    """Extract series information from DICOM files from Insight46

    Files are expected to be organized in the following structure:
    input_dir/
        20??????/
            1.* (study directory)
                1.* (series directory)
    
    The first .dcm file in the series directory is read to extract the series information.
    
    Args:
        input_dir (str): Path to source directory containing DICOM files.
    
    Outputs:
        series_info_{timestamp}.csv: CSV file containing series information.
    """
    print(f"Extracting series info from {input_dir}")
    input_path = Path(input_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_csv = input_path / f"series_info_long_{timestamp}.csv"
    with open(temp_csv, mode='w', newline='') as tcsv:
        writer = csv.writer(tcsv)
        writer.writerow(['path', 'key', 'value'])
    for study_dir in input_path.glob('20??????/1.*'):
        for series_dir in study_dir.glob('1.*'):
            if not series_dir.is_dir():
                continue
            dcm_files = list(series_dir.glob('*.dcm'))
            if not dcm_files:
                print(f"No .dcm file found in {series_dir}")
                continue
            dcm_file = dcm_files[0]
            try:
                ds = pydicom.dcmread(dcm_file)
            except Exception as e:
                print(f"Failed to read DICOM file {dcm_file}: {e}")
                continue

            if "RadiopharmaceuticalInformationSequence" in ds:
                tracer = ds.RadiopharmaceuticalInformationSequence[
                    0
                ].Radiopharmaceutical
            else:
                tracer = ""

            with open(temp_csv, mode='a', newline='') as tcsv:
                writer = csv.writer(tcsv)
                for attr in ATTRIBUTES:
                    if hasattr(ds, attr):
                        writer.writerow([str(dcm_file), attr, str(getattr(ds, attr))])
                writer.writerow([str(dcm_file), 'n_dcm', len(dcm_files)])
                writer.writerow([str(dcm_file), 'tracer', tracer])
    df = pd.read_csv(temp_csv)
    df_wide = df.pivot(index='path', columns='key', values='value').reset_index()
    wide_csv = input_path / f"series_info_{timestamp}.csv"
    df_wide.to_csv(wide_csv, index=False)
    temp_csv.unlink()
    print("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Get DICOM series information.")
    parser.add_argument(
        '--input_dir',
        required=True,
        help='Path to source directory containing DICOM files.'
    )
    args = parser.parse_args()

    main(args.input_dir)
