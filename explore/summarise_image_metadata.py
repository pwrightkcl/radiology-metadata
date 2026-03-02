"""Summarise values in DICOM image metadata file."""
import argparse
from ast import literal_eval

import numpy as np
import pandas as pd
from warnings import simplefilter


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Fields to count value
fields = ['Modality', 'StudyDescription', 'SeriesDescription', 'BodyPartExamined']

# Groups of fields to count together
groups = {'DescPart': ['StudyDescription', 'SeriesDescription', 'BodyPartExamined'],}


def main(args):
    print("Loading DICOM index.")
    di = pd.read_parquet(args.di)

    if args.out:
        excel_filename = args.out
    else:
        excel_filename = args.di.replace('.parquet', '_summary.xlsx')

    with pd.ExcelWriter(excel_filename) as xl:
        print("Writing summary to Excel.")

        print("Errors and warnings.")
        di['error'].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name='errors', index=False)
        di['warnings'].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name='warnings', index=False)

        print("Calculating missingness.")
        isna = di.isna().sum(axis=0).rename('isna')
        empty = di.fillna('ignore').map(lambda x: x.size==0 if isinstance(x, np.ndarray) else not bool(x)
                                        ).sum(axis=0).rename('empty')
        null_strings = ['none', 'null', 'nan', 'n/a', 'na']
        null_string = di.fillna('ignore').astype(str).apply(lambda x: x.str.lower()).isin(null_strings).sum(
            axis=0).rename('null_string')
        missing = pd.concat([isna, empty, null_string], axis=1)
        missing['total'] = missing.sum(axis=1)
        missing.reset_index(names='attribute').to_excel(
            xl, sheet_name='missing', index=False)

        for col in fields:
            print(f"Counting column {col}.")
            if col in di.columns:
                parsed_col = di[col].fillna('NaN').astype(str).replace({'': '_EMPTY_', '[]': '_EMPTY_'})
                parsed_col.value_counts().reset_index().to_excel(xl, sheet_name=col, index=False)
        for desc, cols in groups.items():
            print(f"Counting group {desc} with columns {cols}.")
            if all([x in di.columns for x in cols]):
                parsed_cols = di[cols].fillna('NaN').astype(str).replace({'': '_EMPTY_', '[]': '_EMPTY_'})
                parsed_cols.value_counts().reset_index().to_excel(xl, sheet_name=desc, index=False)

    print("Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Summarise values in DICOM metadata file.")
    parser.add_argument(
        '--di',
        required=True,
        help='DICOM index parquet file.'
    )
    parser.add_argument(
        '--out',
        required=False,
        help='Output Excel filename.'
    )
    main_args = parser.parse_args()
    main(main_args)
