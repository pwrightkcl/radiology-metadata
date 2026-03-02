"""Derive a table of imaging procedures from OMOP by finding all unique children of "imaging" (4180938).

Note that leaves of the tree can be reach by multiple paths, so only the first path is kept.
"""

if __name__ == "__main__":
    print("Notes only. Do not run me.")
    exit(1)


import pandas as pd


c = pd.read_csv('CONCEPT.csv', sep='\t')
# Standard SNOMED procedures
procedures = c.query('domain_id=="Procedure" and vocabulary_id=="SNOMED" and standard_concept=="S"')

cr = pd.read_csv('CONCEPT_RELATIONSHIP.csv', sep='\t')
isa = cr.query('relationship_id=="Is a"')
# Relationships where concept_id_1 is a standard SNOMED procedure
isa_proc = isa.loc[isa['concept_id_1'].isin(procedures['concept_id']), ['concept_id_1', 'concept_id_2']].copy()

level = 1
# Relationships where concept_id_2 (parent) is "imaging" (4180938)
isa_proc_im = isa_proc.loc[isa_proc['concept_id_2']==4180938,:].copy()
isa_proc_im['level'] = level
new_rows = isa_proc_im.shape[0]
while new_rows > 0:
    level += 1
    # Find children of the current table that are not already in the table. Drop duplicates, keeping the one with the
    # lowest parent concept_id.
    isa_proc_im_new = isa_proc.loc[isa_proc['concept_id_2'].isin(isa_proc_im['concept_id_1']) &
                                   ~isa_proc['concept_id_1'].isin(isa_proc_im['concept_id_1']), :
                      ].sort_values(by='concept_id_1').drop_duplicates(subset=['concept_id_1'], keep='first')
    new_rows = isa_proc_im_new.shape[0]
    print(f"Level {level}: {new_rows} new rows")
    if new_rows > 0:
        isa_proc_im_new['level'] = level
        isa_proc_im = pd.concat([isa_proc_im, isa_proc_im_new], ignore_index=True)

# Merge concept names from `procedures` table to concept_id_1 and concept_id_2
proc_im = isa_proc_im[['concept_id_1', 'concept_id_2', 'level']].merge(
    procedures[['concept_id', 'concept_name']], how='left', left_on='concept_id_1', right_on='concept_id'
).drop(columns='concept_id').merge(
    procedures[['concept_id', 'concept_name']], how='left', left_on='concept_id_2', right_on='concept_id',
    suffixes=['_1', '_2']).drop(columns='concept_id')
proc_im.rename(columns={
    'concept_name_1': 'concept_name',
    'concept_name_2': 'parent_concept_name',
    'concept_id_1': 'concept_id',
    'concept_id_2': 'parent_concept_id',
    }, inplace=True)
proc_im = proc_im[['concept_id', 'concept_name', 'parent_concept_id', 'parent_concept_name', 'level']].copy()
# Add a column "children" such that for each row, it counts the number of times the value in 'concept_id' appears in
# 'parent_concept_id' in the same table, replacing NaN with 0.
proc_im['children'] = proc_im['concept_id'].map(proc_im['parent_concept_id'].value_counts()).fillna(0).astype(int)
# Sort by level, then parent_concept_id, then concept_id
proc_im = proc_im.sort_values(by=['level', 'parent_concept_id', 'concept_id']).reset_index(drop=True)
# Save the resulting DataFrame to a CSV file
proc_im.to_csv('imaging_procedures.csv', index=False)
