import pandas as pd

MAPPING_SEX = {'M': 442985, 'F': 442986, 'O': 0}

MAPPING_MODALITY = {
    'CR': 4056681,
    'US': 4037672,
    'DX': 4056681,
    'CT': 4300757,
    'MR': 4013636,
    'MG': 4324693,
    'IO': 1072972,
    'XA': 4299523,
    'PX': 4233227,
    'NM': 4155794,
    'RF': 4195288,
    'PT': 4305790,
}

MAPPING_PROCEDURE_TYPE = {
    'ct spleen': 3006580,
    'spleen ct': 3006580,
}

MAPPING_ANATOMIC_SITE = {
    'spleen': 4302605,
    'abdomen': 37303869,
}

PERSON_COLUMNS = [
    'person_id',
    'gender_concept_id',
    'year_of_birth',
    'month_of_birth',
    'day_of_birth',
    'birth_datetime',
    'race_concept_id',
    'ethnicity_concept_id',
    'location_id',
    'provider_id',
    'care_site_id',
    'person_source_value',
    'gender_source_value',
    'gender_source_concept_id',
    'race_source_value',
    'race_source_concept_id',
    'ethnicity_source_value',
    'ethnicity_source_concept_id',
]

RADIOLOGY_COLUMNS = [
    'radiology_occurrence_id',
    'person_id',
    'radiology_occurrence_date',
    'radiology_occurrence_datetime',
    'modality',
    'manufacturer',
    'protocol_concept_id',
    'protocol_source_value',
    'count_of_series',
    'count_of_images',
    'radiology_note',
    'referral_code',
    'referring_physician',
    'accession_id',
    'trust',
]

VISIT_OCCURRENCE_COLUMNS = [
    'visit_occurrence_id',
    'person_id',
    'visit_concept_id',
    'visit_start_date',
    'visit_start_datetime',
    'visit_end_date',
    'visit_end_datetime',
    'visit_type_concept_id',
    'provider_id',
    'care_site_id',
    'visit_source_value',
    'visit_source_concept_id',
    'admitted_from_concept_id',
    'admitted_from_source_value',
    'discharged_to_concept_id',
    'discharged_to_source_value',
    'preceding_visit_occurrence_id',
]

PROCEDURE_OCCURRENCE_COLUMNS = [
    'procedure_occurrence_id',
    'person_id',
    'procedure_concept_id',
    'procedure_date',
    'procedure_datetime',
    'procedure_type_concept_id',
    'modifier_concept_id',
    'quantity',
    'provider_id',
    'visit_occurrence_id',
    'procedure_source_value',
    'procedure_source_concept_id',
    'modifier_source_value',
]

IMAGE_OCCURRENCE_COLUMNS = [
    'image_occurrence_id',
    'person_id',
    'procedure_occurrence_id',
    'visit_occurrence_id',
    'anatomic_site_concept_id',
    'wadors_uri',
    'local_path',
    'image_occurrence_date',
    'image_study_UID',
    'image_series_UID',
    'modality_concept_id',
    'accession_id',
]

IMAGE_FEATURE_COLUMNS = [
    'image_feature_id',
    'person_id',
    'image_occurrence_id',
    'image_feature_event_field_concept_id',
    'image_feature_event_id',
    'image_feature_concept_id',
    'image_feature_type_concept_id',
    'image_finding_concept_id',
    'image_finding_id',
    'anatomic_site_concept_id',
    'alg_system',
    'alg_datetime',
]

def initialise_person_table():
    return pd.DataFrame(columns=PERSON_COLUMNS)

def initialise_radiology_occurrence_table():
    return pd.DataFrame(columns=RADIOLOGY_COLUMNS)

def initialise_visit_occurrence_table():
    return pd.DataFrame(columns=VISIT_OCCURRENCE_COLUMNS)

def initialise_procedure_occurrence_table():
    return pd.DataFrame(columns=PROCEDURE_OCCURRENCE_COLUMNS)

def initialise_image_occurrence_table():
    return pd.DataFrame(columns=IMAGE_OCCURRENCE_COLUMNS)

def initialise_image_feature_table():
    return pd.DataFrame(columns=IMAGE_FEATURE_COLUMNS)

