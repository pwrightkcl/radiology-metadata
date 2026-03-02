import argparse
import re
from pathlib import Path
from datetime import timedelta

import pandas as pd
from tqdm import tqdm
import timeit


NHS_ISSUER_IDS = ['NHS', '2.16.840.1.113883.2.1.4.1']


def mod11(x):
    """Run modulus 11 test on NHS numbers. Expects a string of ten digits.
    See:
    https://en.wikipedia.org/wiki/NHS_number#Format
    """
    checksum = 0
    for digit in range(9):
        checksum += int(x[digit]) * (10 - digit)
    check_digit = checksum % 11
    check_digit = 11 - check_digit
    if check_digit == 11:
        check_digit = 0
    return check_digit == int(x[9])


def dicom2nhs(di):
    """Search for NHS numbers in all the patient ID columns in the DICOM index.

    Parameters:
        di (DataFrame): DICOM index containing patient ID columns.

    Globals:
        NHS_ISSUER_IDS (list): List of values in IssuerOfPatientID denoting NHS numbers

    Returns:
        patient_index (DataFrame): All Patient IDs, NHS numbers marked
        nhs_index (DataFrame): NHS numbers as integers

    Columns in patient index:
        StudyInstanceUID (str): from DICOM
        n (int): order of occurrence within study
        PatientID (str): from DICOM
        IssuerOfPatientID (str): from DICOM
        is_nhs_number (bool): is an NHS number

    Columns in nhs_index:
        StudyDescription (str): from DICOM
        nhs_number (int): NHS number as integer
    """
    print("Processing patient IDs.")

    len_di0 = len(di)
    di = di.loc[di['NumberOfStudyRelatedInstances'] > 0, :].copy()
    print(f"Of {len_di0} rows, kept {len(di)} with instances "
          f"({len(di)/len_di0*100:.1f}%)")

    len_di0 = len(di)
    di = di.loc[di['StudyInstanceUID'].astype(bool), :].copy()
    print(f"Of {len_di0} rows, kept {len(di)} with StudyInstanceUID "
          f"({len(di)/len_di0*100:.1f}%)")

    len_di0 = len(di)
    di = di.loc[di['PatientID'].astype(bool), :].copy()
    print(f"Of {len_di0} rows, kept {len(di)} with PatientID "
          f"({len(di)/len_di0*100:.1f}%)")

    other_id_cols_from = [c for c in di.columns if c.startswith('OtherPatientIDsSequence.') and not c.endswith('.TypeOfPatientID')]
    other_id_cols_to = [''.join([c.split('.')[2], str(int(c.split('.')[1])+1)]) for c in other_id_cols_from]
    id_cols_from = ['PatientID', 'IssuerOfPatientID'] + other_id_cols_from
    id_cols_to = ['PatientID0', 'IssuerOfPatientID0'] + other_id_cols_to
    pi = di[['StudyInstanceUID'] + id_cols_from].rename(columns=dict(zip(id_cols_from, id_cols_to)))

    print("Expanding DICOM index patient ID columns to long form.")
    len_pi0 = len(pi)
    pi['index'] = range(len(pi))
    pi = pd.wide_to_long(pi, stubnames=['PatientID', 'IssuerOfPatientID'],
                          i='index', j='n').reset_index().drop(columns='index').dropna()
    print(f"Expanded from {len_pi0} rows to {len(pi)}.")

    print("Removing duplicates.")
    len_pi0 = len(pi)
    # Don't look at column 'n', because we want to remove rows with different n but same IDs.
    pi.drop_duplicates(subset=['StudyInstanceUID', 'PatientID', 'IssuerOfPatientID'], inplace=True)
    print(f"Removed {len_pi0-len(pi)} dups ({(len_pi0-len(pi))/len_pi0*100:.1f}%). {len(pi)} rows remain.")

    # Pick NHS numbers
    print("Identifying NHS numbers.")
    nhs_issuer = pi['IssuerOfPatientID'].isin(NHS_ISSUER_IDS)
    pi['id_digits'] = pi['PatientID'].str.replace(r'\D', '', regex=True)
    ten_digit = pi['id_digits'].apply(len) == 10
    for_mod11 = nhs_issuer & ten_digit
    pi['is_nhs_number'] = False
    pi.loc[for_mod11, 'is_nhs_number'] = pi.loc[for_mod11, 'id_digits'].apply(mod11)
    pi['nhs_number'] = 0
    pi.loc[pi['is_nhs_number'], 'nhs_number'] = pi.loc[pi['is_nhs_number'], 'id_digits'].astype(int)
    nhs_per_study = pi.groupby('StudyInstanceUID')['is_nhs_number'].sum()
    print("NHS numbers found per study:")
    print(nhs_per_study.value_counts(normalize=True).map(lambda x: f"{x*100:.3f}%"))

    ni = pi.loc[pi['is_nhs_number'], :].sort_values('n').drop_duplicates(
        subset='StudyInstanceUID')[['StudyInstanceUID', 'nhs_number']].copy()

    pi = pi[['StudyInstanceUID', 'n', 'PatientID', 'IssuerOfPatientID', 'is_nhs_number']].copy()

    print("Done")

    return pi, ni



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Expand patient IDs and mark NHS numbers.")
    parser.add_argument(
        '--di',
        required=True,
        help='DICOM index in parquet format.'
    )
    parser.add_argument(
        '--output_dir',
        required=True,
        help='Output directory'
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Loading DICOM index.")
    tic = timeit.default_timer()
    dicom_index = pd.read_parquet(args.di)
    toc = timeit.default_timer()
    print(f"Loaded {len(dicom_index)} rows in {timedelta(seconds=toc - tic)}.")

    person_index, nhs_index = dicom2nhs(di=dicom_index)

    print("Saving tables.")
    person_index.to_parquet('person_index.parquet')
    nhs_index.to_parquet('person_index_nhs.parquet')

    print("--------------")
    print("---- Done ----")
    print("--------------")
    print("")
