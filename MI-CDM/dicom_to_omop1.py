import argparse
from pathlib import Path
from ast import literal_eval
from datetime import timedelta

import pandas as pd
import timeit

from omop import (
    initialise_image_occurrence_table,
    initialise_person_table,
    initialise_procedure_occurrence_table,
    initialise_visit_occurrence_table,
    initialise_image_feature_table
)


def dicom2concepts(di, map_mis, map_anat, map_proc):
    """Apply mapping from DICOM attributes to OMOP concept IDs for modality, anatomy, and procedure.
    V1: use DICOM index from study-level queries and manual mapping tables

    Parameters:
        di (DataFrame): DICOM metadata index
        map_mis (DataFrame): Table mapping DICOM ModalitiesInStudy strings to OMOP procedure codes
        map_anat (DataFrame): Table mapping DICOM BodyPartExamined and StudyDescription strings to OMOP anatomy codes
        map_proc (DataFrame): Table mapping DICOM StudyDescription strings to OMOP procedure codes

    Returns:
        di (DataFrame): DICOM index dataframe with OMOP concepts

    Required columns for DICOM index:
        NumberOfStudyRelatedInstances
        StudyInstanceUID
        PatientID
        ModalitiesInStudy
        BodyPartExamined
        StudyDescription

    Columns for modality map:
        ModalitiesInStudy (str): DICOM modality code strings as singles or list-as-str, e.g. "CT" or "['CT', 'PR']"
        concept_ids (str): list-as-string of OMOP procedure codes (or "[]")

    Columns for anatomy map:
        BodyPartExamined (str): from DICOM
        StudyDescription (str): from DICOM
        concept_id1...concept_idn (int): OMOP anatomic site codes (first should best describe the study)

    Columns for procedure map:
        StudyDescription (str): from DICOM
        concept_id (str): OMOP procedure codes

    Columns added to DICOM index:
        modality_ids (list): lists of OMOP procedure codes matching Modality
        n_modality_ids (int): number of modality IDs matched
        modality_id (int): single OMOP procedure codes if single match or 0 if zero or multiple matches
        anatomy_id1...anatomy_idn (int): OMOP anatomic site codes
        n_anatomy_ids (int): number of matched anatomy codes per study
        procedure_code (int): OMOP procedure code matching StudyDescription
    """
    print("Mapping DICOM metadata to OMOP concepts.")

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

    # Merge modalities
    di = di.merge(map_mis, how='left', on='ModalitiesInStudy')
    di['concept_ids'].fillna('[]', inplace=True)
    # Convert from list as str to list
    di['modality_ids'] = di['concept_ids'].apply(literal_eval)
    di.drop(columns='concept_ids', inplace=True)
    di['n_modality_ids'] = di['modality_ids'].apply(len)
    di['modality_id'] = 0
    match_modality_single = di['n_modality_ids'].eq(1)
    di.loc[match_modality_single, 'modality_id'] = di.loc[match_modality_single, 'modality_ids'].apply(lambda x: x[0])
    match_modality = di['n_modality_ids'].gt(0).rename('modality')
    n_match_modality = match_modality.sum()
    print(f"Matched ModalitiesInStudy to OMOP procedure code in {n_match_modality} studies "
          f"({n_match_modality/di.shape[0]*100:.1f}%)")
    print("Number of matched modalities per study:")
    print(di['n_modality_ids'].value_counts(normalize=True).map(lambda x: f"{x*100:.3f}%"))

    # Merge anatomy
    di = di.merge(map_anatomy, how='left', on=['BodyPartExamined', 'StudyDescription'])
    n_anatomy_columns = map_anat.shape[1] - 2  # All but the first two columns are `concept_id1`, `concept_id2` etc.
    rename_anatomy_columns = {f'concept_id{x+1}': f'anatomy_id{x+1}' for x in range(n_anatomy_columns)}
    di = di.rename(columns=rename_anatomy_columns)
    fills = {col:0 for col in rename_anatomy_columns.values()}
    di.fillna(fills, inplace=True)

    empty_anatomy_columns = [col for col in rename_anatomy_columns.values() if di[col].sum() == 0]
    if empty_anatomy_columns:
        di.drop(columns=empty_anatomy_columns, inplace=True)
    used_anatomy_columns = [col for col in rename_anatomy_columns.values() if col in di.columns]
    di['n_anatomy_ids'] = di[used_anatomy_columns].gt(0).sum(axis=1)
    match_anatomy = di['n_anatomy_ids'].gt(0).rename('anatomy')
    n_match_anatomy = match_anatomy.sum()
    print(f"Matched BodyPartExamined and StudyDescription to OMOP anatomy code in {n_match_anatomy} studies "
          f"({n_match_anatomy/di.shape[0]*100:.1f}%)")
    print("Number of anatomy matches per study:")
    print(di['n_anatomy_ids'].value_counts(normalize=True).map(lambda x: f"{x*100:.3f}%"))

    # Merge procedure
    di = di.merge(map_proc, how='left', on='StudyDescription')
    di = di.rename(columns={'concept_id': 'procedure_id'})
    di['procedure_id'].fillna(0, inplace=True)
    match_procedure = di['procedure_id'].gt(0).rename('procedure')
    n_match_procedure = match_procedure.sum()
    print(f"Matched StudyDescription to OMOP procedure code in {n_match_procedure} studies "
          f"({n_match_procedure / di.shape[0] * 100:.1f}%)")

    print("")
    print("Summary of matches:")
    print(pd.concat([match_modality, match_procedure, match_anatomy], axis=1).value_counts(normalize=True).map(lambda x: f"{x*100:.3f}%"))
    print("")

    print("Done")
    print("")

    return di


def concepts2tables(di):
    """Construct OMOP tables from DICOM index containing OMOP concept codes.

    Parameters:
        di (DataFrame): DICOM index containing OMOP concept codes

    Returns
        di (DataFrame): DICOM index with OMOP table keys added
        pers (DataFrame): OMOP person table
        vis_occ (DataFrame): OMOP visit occurrence table
        proc_occ (DataFrame): OMOP procedure occurrence table
        im_occ (DataFrame): OMOP image occurrence table
        im_feat (DataFrame): OMOP image feature table

    Required columns for DICOM index:
        PatientID
        StudyDate
        StudyTime
        StudyDescription
        StudyInstanceUID

    Columns added to DICOM index:
        person_id (int): key from person table
    """
    print("Constructing OMOP tables.")
    # ----------------------------------------------
    # ---------------- Person table ----------------
    # ----------------------------------------------
    print("Person table")
    pers = initialise_person_table()
    pers['person_source_value'] = di['PatientID'].drop_duplicates()
    pers['person_id'] = range(1, 1+len(pers))

    # The index currently does not include gender, birthdate, or ethnicity
    pers[['gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'year_of_birth', 'month_of_birth',
            'day_of_birth']] = 0
    pers['gender_source_value'] = ""
    pers['birth_datetime'] = pd.NaT

    # person["gender_concept_id"] = di["PatientSex"].map(MAPPING_SEX)
    # person["gender_source_value"] = di["PatientSex"]
    # person["birth_datetime"] = pd.to_datetime(di["PatientBirthDate"], format="%Y%m%d")
    # person["year_of_birth"] = person["birth_datetime"].dt.year
    # person["month_of_birth"] = person["birth_datetime"].dt.month
    # person["day_of_birth"] = person["birth_datetime"].dt.day
    # person['race_concept_id'] = 0  # set as unknown
    # person['ethnicity_concept_id'] = 0  # set as unknown

    # Map generated person_id back onto PatientID in di
    pre = di.shape[0]
    di = di.merge(pers[['person_source_value', 'person_id']], how='left', left_on='PatientID',
                  right_on='person_source_value')
    if pre != di.shape[0]:
        print(f"Merging person_id to DICOM index changed the number of records from {pre} to {di.shape[0]}.")
        exit(1)
    if di['person_id'].isna().any():
        print(f"Merging person_id to DICOM index created {di['person_id'].isna().sum()} null values.")
        exit(1)
    di.drop(columns='person_source_value', inplace=True)

    # ----------------------------------------------
    # --------- Visit Occurrence table -------------
    # ----------------------------------------------
    print("Visit Occurrence")
    vis_occ = initialise_visit_occurrence_table()
    vis_occ['visit_occurrence_id'] = range(1, 1+len(di))
    vis_occ['person_id'] = di['person_id']
    vis_occ['visit_type_concept_id'] = 32817  # EHR
    vis_occ['visit_concept_id'] = 9202  # outpatient visit
    vis_occ['visit_start_date'] = pd.to_datetime(
        di['StudyDate'], format='%Y%m%d'
    ).dt.date
    vis_occ['visit_start_datetime'] = pd.to_datetime(
        di['StudyDate'] + di['StudyTime'], format='%Y%m%d%H%M%S'
    )
    vis_occ['visit_end_date'] = vis_occ['visit_start_date']
    vis_occ['visit_end_datetime'] = vis_occ['visit_start_datetime']
    vis_occ['provider_id'] = 0  # No provider information available
    vis_occ['care_site_id'] = 0  # No care site information available
    vis_occ['visit_source_value'] = ""
    vis_occ['visit_source_concept_id'] = 0
    vis_occ['admitted_from_concept_id'] = 0
    vis_occ['admitted_from_source_value'] = ""
    vis_occ['discharged_to_concept_id'] = 0
    vis_occ['discharged_to_source_value'] = ""
    vis_occ['preceding_visit_occurrence_id'] = 0

    # ----------------------------------------------
    # --------- Procedure Occurrence table ---------
    # ----------------------------------------------
    print("Procedure Occurrence")
    proc_occ = initialise_procedure_occurrence_table()
    proc_occ['procedure_occurrence_id'] = range(1, 1+len(di))
    proc_occ['person_id'] = di['person_id']
    proc_occ['procedure_concept_id'] = di['procedure_id']
    proc_occ['procedure_date'] = pd.to_datetime(
        di['StudyDate'], format='%Y%m%d').dt.date
    proc_occ['procedure_datetime'] = pd.to_datetime(
        di['StudyDate'] + di['StudyTime'], format='%Y%m%d%H%M%S')
    proc_occ['procedure_type_concept_id'] = 32817  # EHR
    proc_occ['modifier_concept_id'] = 0
    proc_occ['quantity'] = 1  # Could be number of series if available?
    proc_occ['provider_id'] = 0
    proc_occ['visit_occurrence_id'] = vis_occ['visit_occurrence_id']
    proc_occ['procedure_source_value'] = di['StudyDescription']
    proc_occ['procedure_source_concept_id'] = 0
    proc_occ['modifier_source_value'] = 0

    # ----------------------------------------------
    # --------- Image Occurrence table -------------
    # ----------------------------------------------
    print("Image Occurrence")
    im_occ = initialise_image_occurrence_table()
    di['image_occurrence_id'] = range(1, 1+len(di))  # Mirror
    im_occ['image_occurrence_id'] = di['image_occurrence_id']
    im_occ['person_id'] = di['person_id']
    im_occ['procedure_occurrence_id'] = proc_occ['procedure_occurrence_id']
    im_occ['visit_occurrence_id'] = vis_occ['visit_occurrence_id']
    im_occ['anatomic_site_concept_id'] = di['anatomy_id1']
    im_occ['wadors_uri'] = ""
    im_occ['local_path'] = ""
    im_occ['image_occurrence_date'] = pd.to_datetime(di['StudyDate'], format='%Y%m%d').dt.date
    im_occ['image_study_UID'] = di['StudyInstanceUID']
    im_occ['image_series_UID'] = ""  # Not yet indexed
    # im_occ['image_series_UID'] = di['SeriesInstanceUID']
    im_occ['modality_concept_id'] = di['modality_id']
    im_occ['accession_id'] = di['AccessionNumber']

    # ----------------------------------------------
    # ----------- Image Feature table --------------
    # ----------------------------------------------
    print("Image Feature")
    im_feat = initialise_image_feature_table()
    # Reshape di to long form with all extra anatomy IDs (2...n) in one column
    extra_anatomy_columns = [col for col in di.columns if col.startswith('anatomy_id') and col != 'anatomy_id1']
    di_anatomy = di.loc[di['n_anatomy_ids'].gt(1), ['person_id', 'image_occurrence_id'] + extra_anatomy_columns]
    di_anatomy = di_anatomy.melt(id_vars=['person_id', 'image_occurrence_id'],
                                 value_vars=extra_anatomy_columns,
                                 value_name='anatomy_id').drop(columns='variable')
    di_anatomy = di_anatomy.loc[di_anatomy['anatomy_id'].gt(0), :].copy()  # Keep only rows with anatomy_id > 0
    print(f"Expanded {di['n_anatomy_ids'].gt(1).sum()} studies with multiple anatomy matches to {len(di_anatomy)} "
          f"anatomy labels.")
    im_feat[['person_id', 'image_occurrence_id']] = di_anatomy[['person_id', 'image_occurrence_id']]
    im_feat['image_feature_id'] = range(1, 1 + len(im_feat))
    im_feat['anatomic_site_concept_id'] = di_anatomy['anatomy_id']
    im_feat[[
    'image_feature_event_field_concept_id',
    'image_feature_event_id',
    'image_feature_concept_id',
    'image_feature_type_concept_id',
    'image_finding_concept_id',
    'image_finding_id']] = 0
    im_feat['alg_system'] = ""
    im_feat['alg_datetime'] = pd.NaT

    print("Done")
    print("")

    return di, pers, vis_occ, proc_occ, im_occ, im_feat


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Map DICOM index to OMOP tables.")
    parser.add_argument(
        '--di',
        required=True,
        help='DICOM index in parquet format.'
    )
    parser.add_argument(
        '--modality',
        required=True,
        help='CSV containing mapping from DICOM ModalitiesInStudy strings to OMOP procedure codes.'
    )
    parser.add_argument(
        '--anatomy',
        required=True,
        help='CSV containing mapping from DICOM StudyDescription and BodyPartExamined strings to OMOP anatomy codes.'
    )
    parser.add_argument(
        '--procedure',
        required=True,
        help='CSV containing mapping from DICOM StudyDescription to OMOP procedure codes.'
    )
    parser.add_argument(
        '--output_dir',
        required=True,
        help='Output directory'
    )

    args = parser.parse_args()

    print("Loading DICOM index.")
    tic = timeit.default_timer()
    dicom_index = pd.read_parquet(args.di)
    toc = timeit.default_timer()
    print(f"Loaded {len(dicom_index)} rows in {timedelta(seconds=toc - tic)}.")

    print("Loading mapping CSV files.")
    map_modalities_in_study = pd.read_csv(args.modality)
    map_anatomy = pd.read_csv(args.anatomy)
    map_procedure = pd.read_csv(args.procedure)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using output directory {output_dir}")

    dicom_index_concepts = dicom2concepts(di=dicom_index, map_mis=map_modalities_in_study, map_anat=map_anatomy, map_proc=map_procedure)
    print("Saving DICOM index with concepts")
    dicom_index_concepts.to_parquet(output_dir / 'dicom2omop.parquet')

    (
        dicom_index_tables,
        person,
        visit_occurrence,
        procedure_occurrence,
        image_occurrence,
        image_feature
    ) = concepts2tables(di=dicom_index_concepts)

    # Save tables
    print("Saving OMOP tables and DICOM index with table keys.")
    dicom_index_tables.to_parquet(output_dir / 'dicom2omop.parquet')
    person.to_parquet(output_dir / 'person.parquet')
    visit_occurrence.to_parquet(output_dir / 'visit_occurrence.parquet')
    procedure_occurrence.to_parquet(output_dir / 'procedure_occurrence.parquet')
    image_occurrence.to_parquet(output_dir / 'image_occurrence.parquet')
    image_feature.to_parquet(output_dir / 'image_feature.parquet')
    print("--------------")
    print("---- Done ----")
    print("--------------")
    print("")
