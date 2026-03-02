"""Summarise values in DICOM metadata file."""
import argparse
from ast import literal_eval

import pandas as pd
from warnings import simplefilter


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Fields to count
fields = ['StudyDate', 'ModalitiesInStudy',
          'StudyDescription', 'IssuerOfPatientID', 'BodyPartExamined']

# Groups of fields with their group labels
groups = [(['BodyPartExamined', 'StudyDescription'], 'PartDesc')]


def main(args):
    print("Loading DICOM index.")
    di = pd.read_parquet(args.di)
    if args.ctmr:
        di = di.loc[(di['Modality'] == 'CT') | (di['Modality'] == 'MR'), :]

    if args.out:
        excel_filename = args.out
    else:
        excel_filename = args.di.replace('.parquet', '_summary.xlsx')

    with pd.ExcelWriter(excel_filename) as xl:
        print("Writing summary to Excel.")
        di.isnull().sum(axis=0).to_excel(xl, sheet_name='missing', index=True)
        di.fillna('NA').map(bool).sum(axis=0).to_excel(xl, sheet_name='empty', index=True)

        di['has_series'] = di['NumberOfStudyRelatedSeries'] > 0
        di['has_instances'] = di['NumberOfStudyRelatedInstances'] > 0
        has = di[['has_series', 'has_instances']].value_counts(dropna=False).reset_index()
        has.to_excel(xl, sheet_name='has', index=False)
        # For subsequent counts, only consider studies that have instances
        di = di.loc[di['has_instances'], :].copy()

        di['year'] = di['StudyDate'].str[:4]
        di['year'].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name='year', index=False)

        for string_field in ['BodyPartExamined', 'StudyDescription', 'ModalitiesInStudy']:
            di[string_field] = di[string_field].fillna('_missing_')
            di.loc[di[string_field] == '', string_field] = '_empty_'
            di[string_field] = di[string_field].map(lambda x: x.encode('unicode_escape').decode('utf-8'))

        for col in fields:
            print(col)
            if col in di.columns:
                di[col].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name=col, index=False)
        for cols, desc in groups:
            print(desc)
            if all([di.columns.str.contains(x).any() for x in cols]):
                di[cols].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name=desc, index=False)

        # Special pivot tables by modality for study-level queries
        print("Listing modalities.")
        modalities = []
        for m in di['ModalitiesInStudy'].unique():
            if m.startswith('['):
                modalities.extend(literal_eval(m))
            else:
                modalities.append(m)
        modalities = list(set(modalities))
        modalities = [m for m in modalities if m]  # I was getting an empty string for some reason

        print("Identifying modality occurrences ", end="")
        for modality in modalities:
            print(".", end="")
            # Use enquoted modality to find its uses in list strings and comparison for single modality strings.
            # e.g. "['CR', 'SR']" or "CR"
            # Otherwise wonky modalities like "R" get many spurious matches.
            di[modality] = di['ModalitiesInStudy'].str.contains(f"'{modality}'") | (di['ModalitiesInStudy'] == modality)
        print()
        di[modalities].sum(axis=0).sort_values(ascending=False).to_excel(
            xl, sheet_name='Modality', index=True, index_label='modality', header=['count'])

        print("Summarising ", end="")
        for modality in ['DX', 'CR', 'US', 'CT', 'MR']:
            print(f"{modality} ", end="")
            di.loc[di[modality], ['BodyPartExamined', 'StudyDescription']
            ].value_counts(dropna=False).reset_index().to_excel(xl, sheet_name=f'PartDesc_{modality}', index=False)
        print()
    print("Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Summarise values in DICOM metadata file.")
    parser.add_argument(
        '--di',
        required=True,
        help='DICOM index parquet file.'
    )
    parser.add_argument(
        '--ctmr',
        required=False,
        action='store_true',
        help='Only include Modality of CT or MR.'
    )
    parser.add_argument(
        '--out',
        required=False,
        help='Output Excel filename.'
    )
    main_args = parser.parse_args()
    main(main_args)
