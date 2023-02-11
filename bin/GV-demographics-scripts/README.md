Nov. 2022

For the glucose variability paper involving the OPENonOH and the OpenAPS datasets the following processing steps are taken:
1. input:
    - OpenAPS: OpenAPS complete_patient_statistics.xlsx
    - OPENonOH: FINAL n=75 statistics processed.xlsx (BG data) and n=75_deduped_demographics.xlsx (demographic data, including information on total insulin doses)

2. processing OPENonOH data into the same format as the OpenAPS data:
    - scripts: preprocess_OPENonOH.ipynb 
    - output: OPENonOH complete_patient_statistics.xlsx

    In Nov. 2022 there was a bug fix on taking the children's demographic information properly into account. The bug fix is implemented in the script preprocess_OPENonOH.ipynb. The output file OPENonOH complete_patient_statistics.xlsx has been updated accordingly.

3. dataset_comp.ipynb to produce plots and statistics for the paper showing OpenAPS and OPENonOH data in comparison.
dataset_comp.ipynb has been adapted from demographics_EDA_shared.ipynb.

4. plots for the 2nd gv paper:
    - gv_kpi_distributions.ipynb
    - data_comp.ipynb
    - output: many figures :) a subset of which made it into the paper

5. timeseries analysis:
    - step 1: hourly_timeseries.ipynb
    - step 2: Timeseries-analysis-for-CGM-data-2nd-gv-paper_OPENonOH_only.ipynb
    - output: several plots (that are saved manually from the Jupyter notebook)
    [- previous version: Timeseries-analysis-for-CGM-data-2nd-gv-paper.ipynb]

    Timeseries-analysis-for-CGM-data-2nd-gv-paper_OPENonOH_only.ipynb is a cleaned-up version of Timeseries-analysis-for-CGM-data-2nd-gv-paper.ipynb, which contains code to plot both OpenAPS and OPENonOH data. 
    Both were taken from Timeseries-analysis-for-CGM-data-shared.ipynb, which was used for the 1st gv paper.