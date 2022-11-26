Nov. 2022

For the glucose variability paper involving the OPENonOH and the OpenAPS datasets the following processing steps are taken:
1. input:
    - OpenAPS: OpenAPS complete_patient_statistics.xlsx
    - OPENonOH: FINAL n=75 statistics processed.xlsx (BG data) and n=75_deduped_demographics.xlsx (demographic data, including information on total insulin doses)

2. processing OPENonOH data into the same format as the OpenAPS data:
    - scripts: preprocess_OPENonOH.ipynb 
    - output: OPENonOH complete_patient_statistics.xlsx

3. dataset_comp.ipynb to produce plots and statistics for the paper showing OpenAPS and OPENonOH data in comparison.
dataset_comp.ipynb has been adapted from demographics_EDA_shared.ipynb.