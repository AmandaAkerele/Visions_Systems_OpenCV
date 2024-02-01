# Visions_Systems_OpenCV

# Computer Vision Code Examples Readme

Welcome to the Computer Vision Code Examples repository! This readme provides an overview of the notebook and descriptions related to various computer vision tasks. Each section demonstrates the implementation of different computer vision algorithms using OpenCV, a popular computer vision library in Python.

## Table of Contents

- [Introduction](#introduction)
- [Object Detection using YOLO](#object-detection-using-yolo)
- [Image Segmentation with Watershed Algorithm](#image-segmentation-with-watershed-algorithm)
- [Implementing SIFT Algorithm](#implementing-sift-algorithm)
- [Image Pyramid Implementation](#image-pyramid-implementation)
- [Harris Corner Detection Algorithm](#harris-corner-detection-algorithm)

## Introduction

This repository contains code examples showcasing the implementation of various computer vision algorithms using OpenCV. Each section includes a description of the algorithm, the code implementation, and an explanation of its functionality.

## Object Detection using YOLO

The YOLO (You Only Look Once) algorithm is used for real-time object detection. The provided code demonstrates how to load a pre-trained YOLOv3 model, perform object detection on images, and draw bounding boxes around detected objects. The COCO dataset's class labels are used to identify different object classes.

## Image Segmentation with Watershed Algorithm

The Watershed algorithm is employed for image segmentation, separating an image into distinct regions. The code preprocesses the image, detects foreground and background regions, and applies the Watershed algorithm to segment the image. Bounding boxes are marked around the segmented regions to visualize the algorithm's effectiveness.

## Implementing SIFT Algorithm

The SIFT (Scale-Invariant Feature Transform) algorithm is used for feature detection and matching. The code demonstrates how to detect keypoints and compute descriptors using the SIFT algorithm. Detected keypoints are visualized by drawing circles on the image.

## Image Pyramid Implementation

The Image Pyramid technique involves creating multi-scale representations of an image by repeatedly downsampling it. The code example shows how to generate an image pyramid using the cv2.pyrDown() function and visualize the different levels of the pyramid.

## Harris Corner Detection Algorithm

The Harris corner detection algorithm identifies corner points in images. The code explains the algorithm's steps, including grayscale conversion, gradient calculation, Harris response calculation, non-maximum suppression, and thresholding. Detected corners are highlighted in the output image.

## Usage

Each code example is self-contained and can be run independently. To use the code snippets:

1. Ensure you have OpenCV and other required libraries installed.
2. Replace file paths with the paths to your images if needed.
3. Run the code in a Python environment.

Feel free to experiment with the code, modify parameters, and use your own images for testing.

## Contributing
Contributions to this repository are welcome! If you have additional code examples, improvements, or suggestions, please submit a pull request.

---

Thank you for exploring the Computer Vision Code Examples repository! I hope these examples help you in your computer vision journey.


/////////////////////////////////////////////////////////////////


import pandas as pd

# Define constant values
constants = {
    "INDICATOR_CODE": "033",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "IMPROVEMENT_IND_CODE": '999',
    "COMPARE_IND_CODE": '999',
    "DATA_PERIOD_CODE": "033",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

# Merge and filter for los_organization_comparative_trend
los_org_comparative_trend_av1 = pd.merge(los_corp['CORP_ID'], los_org_compare_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left')
los_org_comparative_trend_a = pd.merge(los_org_comparative_trend_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], los_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']], on=['CORP_ID'], how='left')
merged_df = pd.merge(los_org_comparative_trend_a, ed_nacrs_flag_1, on='CORP_ID', how='left', indicator=True)
los_org_comparative_trend = merged_df[merged_df['_merge'] == 'left_only']
los_org_comparative_trend = los_org_comparative_trend.drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLAG'])
display(los_org_comparative_trend)

# Merge and filter for tpia_organization_comparative_trend
tpia_org_comparative_trend_av1 = pd.merge(tpia_corp['CORP_ID'], tpia_org_compare_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left')
tpia_org_comparative_trend_a = pd.merge(tpia_org_comparative_trend_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], tpia_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']], on=['CORP_ID'], how='left')
merged_df = pd.merge(tpia_org_comparative_trend_a, ed_nacrs_flag_1, on='CORP_ID', how='left', indicator=True)
tpia_org_comparative_trend = merged_df[merged_df['_merge'] == 'left_only']
tpia_org_comparative_trend = tpia_org_comparative_trend.drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLAG'])
display(tpia_org_comparative_trend)

# Rename columns for los_region and tpia_region
los_region_renamed = los_region.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
tpia_region_renamed = tpia_region.rename(columns={'NEW_REGION_ID': 'REGION_ID'})

# Merge and filter for los_region_comparative_trend
los_region_comparative_trend_av1 = pd.merge(los_region_renamed['REGION_ID'], los_region_compare_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left')
los_region_comparative_trend = pd.merge(los_region_comparative_trend_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], los_region_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']], on=['REGION_ID'], how='left')
display(los_region_comparative_trend)

# Merge and filter for tpia_region_comparative_trend
tpia_region_comparative_trend_av1 = pd.merge(tpia_region_renamed['REGION_ID'], tpia_region_compare_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left')
tpia_region_comparative_trend = pd.merge(tpia_region_comparative_trend_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], tpia_region_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']], on=['REGION_ID'], how='left')
display(tpia_region_comparative_trend)

# Load and process hsp_last_organization_fact_los
sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")
hsp_last_organization_fact_los_a = sas.sasdata(table='hsp_last_organization_fact33', libref='hsp_last') 
hsp_last_organization_fact_los = hsp_last_organization_fact_los_a.to_df()
hsp_last_organization_fact_los = hsp_last_organization_fact_los.rename(columns=lambda x: x.upper())
hsp_last_organization_fact_los = hsp_last_organization_fact_los[hsp_last_organization_fact_los.FISCAL_YEAR_WH_ID != 17]
display(hsp_last_organization_fact_los)

# Load and process hsp_last_organization_fact_tpia
hsp_last_organization_fact_tpia_a = sas.sasdata(table='hsp_last_organization_fact34', libref='hsp_last') 
hsp_last_organization_fact_tpia = hsp_last_organization_fact_tpia_a.to_df()
hsp_last_organization_fact_tpia = hsp_last_organization_fact_tpia.rename(columns=lambda x: x.upper())
hsp_last_organization_fact_tpia = hsp_last_organization_fact_tpia[hsp_last_organization_fact_tpia.FISCAL_YEAR_WH_ID != 17]
display(hsp_last_organization_fact_tpia)

# Update organization IDs
hsp_last_organization_fact_los['ORGANIZATION_ID'] = hsp_last_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81180 if x == 5085 else (81263 if x == 5049 else x))
hsp_last_organization_fact_tpia['ORGANIZATION_ID'] = hsp_last_organization_fact_tpia['ORGANIZATION_ID'].apply(lambda x: 81180 if x == 5085 else (81263 if x == 5049 else x))
display(hsp_last_organization_fact_los)
display(hsp_last_organization_fact_tpia)

# Filter test dataframes
test_los = hsp_last_organization_fact_los[hsp_last_organization_fact_los['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_los)

test_tpia = hsp_last_organization_fact_tpia[hsp_last_organization_fact_tpia['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_tpia)

# Define a common constant dictionary for preparation
constant_common = {
    "INDICATOR_CODE": "033",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_CODE": "033",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

# Function to prepare dataframes
def prepare_dataframe(df, id_col, indicator_value_col):
    df_updated = df.rename(columns={id_col: 'ORGANIZATION_ID', indicator_value_col: 'INDICATOR_VALUE'})
    for col, value in constant_common.items():
        df_updated[col] = value
    df_updated = df_updated.reindex(columns=hsp_last_organization_fact_los.columns)
    return df_updated

# Prepare dataframes without "prepared" in their names
los_org_comparative_trend_v2 = prepare_dataframe(los_org_comparative_trend, 'CORP_ID', 'PERCENTILE_90')
los_region_comparative_trend_v2 = prepare_dataframe(los_region_comparative_trend, 'REGION_ID', 'PERCENTILE_90')
tpia_org_comparative_trend_v2 = prepare_dataframe(tpia_org_comparative_trend, 'CORP_ID', 'PERCENTILE_90')
tpia_region_comparative_trend_v2 = prepare_dataframe(tpia_region_comparative_trend, 'REGION_ID', 'PERCENTILE_90')
los_prov_v2 = prepare_dataframe(los_prov, 'PROVINCE_ID', 'PERCENTILE_90')
tpia_prov_v2 = prepare_dataframe(tpia_prov, 'PROVINCE_ID', 'PERCENTILE_90')
los_peer_v2 = prepare_dataframe(los_peer, 'peer_id', 'PERCENTILE_90')
tpia_peer_v2 = prepare_dataframe(tpia_peer, 'peer_id', 'PERCENTILE_90')
LOS_nt_v2 = prepare_dataframe(LOS_nt, 'NATIONAL_ID', 'PERCENTILE_90')
TPIA_nt_v2 = prepare_dataframe(TPIA_nt, 'NATIONAL_ID', 'PERCENTILE_90')

# Concatenate all DataFrames
final_df_v2 = pd.concat([hsp_last_organization_fact_los, los_org_comparative_trend_v2, los_region_comparative_trend_v2,
                      tpia_org_comparative_trend_v2, tpia_region_comparative_trend_v2, los_prov_v2,
                      tpia_prov_v2, los_peer_v2, tpia_peer_v2, LOS_nt_v2, TPIA_nt_v2], ignore_index=True)
final_df_v2 = final_df_v2.sort_values(['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
display(final_df_v2)



////////////////////////
NEW


import pandas as pd

# Filter and process los_org_com_trd
los_org_com_trd = los_corp[['CORP_ID']].merge(los_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left', indicator=True)
los_org_com_trd = los_org_com_trd[los_org_com_trd['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(los_org_com_trd)

# Filter and process tpia_org_com_trd
tpia_org_com_trd = tpia_corp[['CORP_ID']].merge(tpia_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left', indicator=True)
tpia_org_com_trd = tpia_org_com_trd[tpia_org_com_trd['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(tpia_org_com_trd)

# Rename columns for los_reg and tpia_reg
los_reg_a = los_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
tpia_reg_a = tpia_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})

# Filter and process los_reg_com_trd
los_reg_com_trd = los_reg_a[['REGION_ID']].merge(los_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left', indicator=True)
los_reg_com_trd = los_reg_com_trd[los_reg_com_trd['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(los_reg_com_trd)

# Filter and process tpia_reg_com_trd
tpia_reg_com_trd = tpia_reg_a[['REGION_ID']].merge(tpia_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left', indicator=True)
tpia_reg_com_trd = tpia_reg_com_trd[tpia_reg_com_trd['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(tpia_reg_com_trd)


///

checking if code is working 

# los_org_com_trd_av1
los_corp_subset = los_corp[['CORP_ID']]
los_org_cmp_subset = los_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']]
los_org_com_trd_av1 = pd.merge(los_corp_subset, los_org_cmp_subset, on=['CORP_ID'], how='left')

# los_org_com_trd_a
los_org_com_trd_a = pd.merge(los_org_com_trd_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']],
                              los_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']],
                              on=['CORP_ID'], how='left')

# los_org_com_trd
merged_df = pd.merge(los_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
los_org_com_trd = merged_df[merged_df['_merge'] == 'left_only']
los_org_com_trd = los_org_com_trd.drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(los_org_com_trd)

# tpia_org_com_trd_av1
tpia_corp_subset = tpia_corp[['CORP_ID']]
tpia_org_cmp_subset = tpia_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']]
tpia_org_com_trd_av1 = pd.merge(tpia_corp_subset, tpia_org_cmp_subset, on=['CORP_ID'], how='left')

# tpia_org_com_trd_a
tpia_org_com_trd_a = pd.merge(tpia_org_com_trd_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']],
                              tpia_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']],
                              on=['CORP_ID'], how='left')

# tpia_org_com_trd
merged_df = pd.merge(tpia_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
tpia_org_com_trd = merged_df[merged_df['_merge'] == 'left_only']
tpia_org_com_trd = tpia_org_com_trd.drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(tpia_org_com_trd)

# los_reg_com_trd_av1
los_reg_a = los_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
los_reg_com_trd_av1 = pd.merge(los_reg_a['REGION_ID'],
                                los_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']],
                                on=['REGION_ID'], how='left')

# los_reg_com_trd
los_reg_com_trd = pd.merge(los_reg_com_trd_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']],
                           los_reg_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']],
                           on=['REGION_ID'], how='left')
display(los_reg_com_trd)

# tpia_reg_com_trd_av1
tpia_reg_a = tpia_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
tpia_reg_com_trd_av1 = pd.merge(tpia_reg_a['REGION_ID'],
                                tpia_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']],
                                on=['REGION_ID'], how='left')

# tpia_reg_com_trd
tpia_reg_com_trd = pd.merge(tpia_reg_com_trd_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']],
                           tpia_reg_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']],
                           on=['REGION_ID'], how='left')
display(tpia_reg_com_trd)

//////rabs rowwww

los_org_com_trd_av1=pd.merge(los_corp['CORP_ID'], los_org_cmp_a[['CORP_ID','COMPARE_IND_CODE','PERCENTILE_90']],on=['CORP_ID'],how='left')
los_org_com_trd_a=pd.merge(los_org_com_trd_av1[['CORP_ID','PERCENTILE_90','COMPARE_IND_CODE']], los_org_trend_b[['CORP_ID','IMPROVEMENT_IND_CODE']],on=['CORP_ID'],how='left')
merged_df=pd.merge(los_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
los_org_com_trd=merged_df[merged_df['_merge']=='left_only']
los_org_com_trd=los_org_com_trd.drop(columns=['_merge','SUBMISSION_FISCAL_YEAR','NACRS_ED_FLG'])
display(los_org_com_trd)

tpia_org_com_trd_av1=pd.merge(tpia_corp['CORP_ID'], tpia_org_cmp_a[['CORP_ID','COMPARE_IND_CODE','PERCENTILE_90']],on=['CORP_ID'],how='left')
tpia_org_com_trd_a=pd.merge(tpia_org_com_trd_av1[['CORP_ID','PERCENTILE_90','COMPARE_IND_CODE']], tpia_org_trend_b[['CORP_ID','IMPROVEMENT_IND_CODE']],on=['CORP_ID'],how='left')
merged_df=pd.merge(tpia_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
tpia_org_com_trd=merged_df[merged_df['_merge']=='left_only']
tpia_org_com_trd=tpia_org_com_trd.drop(columns=['_merge','SUBMISSION_FISCAL_YEAR','NACRS_ED_FLG'])
display(tpia_org_com_trd)

los_reg_a=los_reg.rename(columns={'NEW_REGION_ID' : 'REGION_ID'})
los_reg_com_trd_av1=pd.merge(los_reg_a['REGION_ID'], los_reg_cmp_a[['REGION_ID','COMPARE_IND_CODE','PERCENTILE_90']],on=['REGION_ID'],how='left')
los_reg_com_trd=pd.merge(los_reg_com_trd_av1[['REGION_ID','PERCENTILE_90','COMPARE_IND_CODE']], los_reg_trend_b[['REGION_ID','IMPROVEMENT_IND_CODE']],on=['REGION_ID'],how='left')
display(los_reg_com_trd)

tpia_reg_a=tpia_reg.rename(columns={'NEW_REGION_ID' : 'REGION_ID'})
tpia_reg_com_trd_av1=pd.merge(tpia_reg_a['REGION_ID'], tpia_reg_cmp_a[['REGION_ID','COMPARE_IND_CODE','PERCENTILE_90']],on=['REGION_ID'],how='left')
tpia_reg_com_trd=pd.merge(tpia_reg_com_trd_av1[['REGION_ID','PERCENTILE_90','COMPARE_IND_CODE']], tpia_reg_trend_b[['REGION_ID','IMPROVEMENT_IND_CODE']],on=['REGION_ID'],how='left')
display(tpia_reg_com_trd)

sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")
hsp_ind_organization_fact_los_a = sas.sasdata(table='hsp_ind_organization_fact33', libref='hsp_last') 
hsp_ind_organization_fact_los=hsp_ind_organization_fact_los_a.to_df()
hsp_ind_organization_fact_los=hsp_ind_organization_fact_los.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_los=hsp_ind_organization_fact_los[hsp_ind_organization_fact_los.FISCAL_YEAR_WH_ID !=17]
display(hsp_ind_organization_fact_los)


hsp_ind_organization_fact_tpia_a = sas.sasdata(table='hsp_ind_organization_fact34', libref='hsp_last') 
hsp_ind_organization_fact_tpia=hsp_ind_organization_fact_tpia_a.to_df()
hsp_ind_organization_fact_tpia=hsp_ind_organization_fact_tpia.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_tpia=hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia.FISCAL_YEAR_WH_ID !=17]
display(hsp_ind_organization_fact_tpia)

sas.saslib('hsp_org', path=r"M:\Groups\eReporting\OurHealthSystem\Lookup Tables")
hsp_organization_ext_a = sas.sasdata(table='hsp_organization_ext', libref='hsp_org') 
hsp_organization_ext=hsp_organization_ext_a.to_df()
hsp_organization_ext=hsp_organization_ext.rename(columns=lambda x: x.upper())
display(hsp_organization_ext)


hsp_ind_organization_fact_los['ORGANIZATION_ID']=hsp_ind_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81180 if x==5085 else x)
hsp_ind_organization_fact_los['ORGANIZATION_ID']=hsp_ind_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81263 if x==5049 else x)
display(hsp_ind_organization_fact_los)

hsp_ind_organization_fact_tpia['ORGANIZATION_ID']=hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].apply(lambda x: 81180 if x==5085 else x)
hsp_ind_organization_fact_tpia['ORGANIZATION_ID']=hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].apply(lambda x: 81263 if x==5049 else x)
display(hsp_ind_organization_fact_tpia)


test=hsp_ind_organization_fact_los[hsp_ind_organization_fact_los['ORGANIZATION_ID'].isin([81118,5049,5085,81180,81263])]
display(test)

test2=hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].isin([81118,5049,5085,81180,81263])]
display(test2)



# Constant values for org, reg
constant_org_regLOS = {
    "INDICATOR_CODE": "033",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_CODE": "033",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}
constant_org_regTPIA = {
    "INDICATOR_CODE": "034",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_CODE": "034",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}
# Function to prepare DataFrame
def prepare_org_regLOS(df, id_col):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    for col, value in constant_org_regLOS.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_los.columns)
    return df

# Prepare data
los_org_prepared = prepare_org_regLOS(los_org_com_trd, 'CORP_ID')
los_reg_prepared = prepare_org_regLOS(los_reg_com_trd, 'REGION_ID')

# Function to prepare DataFrame
def prepare_org_regTPIA(df, id_col):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    for col, value in constant_org_regTPIA.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_tpia.columns)
    return df
tpia_org_prepared = prepare_org_regTPIA(tpia_org_com_trd, 'CORP_ID')
tpia_reg_prepared = prepare_org_regTPIA(tpia_reg_com_trd, 'REGION_ID')

constant_los = {
    "INDICATOR_CODE": "033",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "IMPROVEMENT_IND_CODE": '999',
    "COMPARE_IND_CODE": '999',
    "DATA_PERIOD_CODE": "033",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}
constant_tpia = {
    "INDICATOR_CODE": "034",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "IMPROVEMENT_IND_CODE": '999',
    "COMPARE_IND_CODE": '999',
    "DATA_PERIOD_CODE": "034",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

def prepare_los(df, id_col):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    for col, value in constant_los.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_los.columns)
    return df

los_prov_prepared = prepare_los(los_prov, 'PROVINCE_ID')
los_peer_prepared = prepare_los(los_peer, 'peer_id')
los_nat_prepared = prepare_los(LOS_nt, 'NATIONAL_ID')

def prepare_tpia(df, id_col):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    for col, value in constant_tpia.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_tpia.columns)
    return df
tpia_prov_prepared = prepare_tpia(tpia_prov, 'PROVINCE_ID')
tpia_peer_prepared =prepare_tpia(tpia_peer, 'peer_id')
tpia_nat_prepared =prepare_tpia(TPIA_nt, 'NATIONAL_ID')

# Concatenate all DataFrames
hsp_ind_organization_fact_los_final_a = pd.concat([hsp_ind_organization_fact_los, los_org_prepared,los_reg_prepared, los_prov_prepared, los_peer_prepared, los_nat_prepared ], ignore_index=True)
hsp_ind_organization_fact_los_final=hsp_ind_organization_fact_los_final_a.sort_values(['ORGANIZATION_ID','FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_los_final)

hsp_ind_organization_fact_tpia_final_a = pd.concat([hsp_ind_organization_fact_tpia, tpia_org_prepared,tpia_reg_prepared, tpia_prov_prepared, tpia_peer_prepared, tpia_nat_prepared ], ignore_index=True)
hsp_ind_organization_fact_tpia_final=hsp_ind_organization_fact_tpia_final_a.sort_values(['ORGANIZATION_ID','FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_tpia_final)



# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_los = counts_los[counts_los < 5].index

# Define a function to get missing years
def missing_years_los(org_id):
    existing_years = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Prepare dummy data
dummy_datalos = {column: [] for column in hsp_ind_organization_fact_los_final.columns}


# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in orgs_to_add_los:
    missing_years = missing_years_los(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_los_final.columns:
            if column == 'ORGANIZATION_ID':
                dummy_datalos[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_datalos[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_datalos[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_datalos[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_datalos[column].append('999')
            elif column=='IMPROVEMENT_IND_CODE':
                dummy_datalos[column].append('999')
            elif column =='COMPARE_IND_CODE':
                dummy_datalos[column].append('999')
            elif column =='DATA_PERIOD_CODE':
                dummy_datalos[column].append('033')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_datalos[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_los_final[column].dtype == 'int64' or hsp_ind_organization_fact_los_final[column].dtype == 'float64':
                        dummy_datalos[column].append(0)  # Default value for numeric columns
                    else:
                        dummy_datalos[column].append('999')  # Default value for string columns
                
# Convert dummy data to DataFrame
dummy_los = pd.DataFrame(dummy_datalos)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_los_33_a = pd.concat([hsp_ind_organization_fact_los_final, dummy_los], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_los_33_b = hsp_ind_organization_fact_los_33_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_los_33_c = hsp_ind_organization_fact_los_33_b[hsp_ind_organization_fact_los_33_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
display(hsp_ind_organization_fact_los_33_c)
# Save the updated DataFrame to a new CSV file, if needed
# hsp_ind_organization_fact_los_final.to_csv('updated_data.csv', index=False)


# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Define a function to get missing years
def get_missing_years(org_id):
    existing_years = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Prepare dummy data
dummy_datatpia = {column: [] for column in hsp_ind_organization_fact_tpia_final.columns}


# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in org_to_add_tpia:
    missing_years = get_missing_years(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column == 'ORGANIZATION_ID':
                dummy_datatpia[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_datatpia[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_datatpia[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_datatpia[column].append('034')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_datatpia[column].append('999')
            elif column=='IMPROVEMENT_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='COMPARE_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='DATA_PERIOD_CODE':
                dummy_datatpia[column].append('034')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_datatpia[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_tpia_final[column].dtype == 'int64' or hsp_ind_organization_fact_tpia_final[column].dtype == 'float64':
                        dummy_datatpia[column].append(0)  # Default value for numeric columns
                    else:
                        dummy_datatpia[column].append('999')  # Default value for string columns
                
# Convert dummy data to DataFrame
dummy_tpia = pd.DataFrame(dummy_datatpia)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_tpia_34_a = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_tpia], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_tpia_34_b = hsp_ind_organization_fact_tpia_34_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_tpia_34_c = hsp_ind_organization_fact_tpia_34_b[hsp_ind_organization_fact_tpia_34_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]
display(hsp_ind_organization_fact_tpia_34_c)
# Save the updated DataFrame to a new CSV file, if needed
# hsp_ind_organization_fact_tpia_final.to_csv('updated_data.csv', index=False)


test1=hsp_ind_organization_fact_tpia_34_c[hsp_ind_organization_fact_tpia_34_c['ORGANIZATION_ID'].isin([693,2044,81410,7020])]
display(test1)
test2=hsp_ind_organization_fact_los_33_c[hsp_ind_organization_fact_los_33_c['ORGANIZATION_ID'].isin([693,2044,81410,300])]
display(test2)


////// shorten Rabs py
# Common function to prepare data
def prepare_data(source_df, id_col, indicator_code):
    merged_av1 = pd.merge(source_df[id_col], source_df[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left')
    merged_a = pd.merge(merged_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], source_df[['CORP_ID', 'IMPROVEMENT_IND_CODE']], on=['CORP_ID'], how='left')
    merged_df = pd.merge(merged_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
    result_df = merged_df[merged_df['_merge']=='left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
    display(result_df)

# Prepare LOS data
prepare_data(los_corp, 'CORP_ID', '033')

# Prepare TPIA data
prepare_data(tpia_corp, 'CORP_ID', '034')

# Prepare LOS Region data
prepare_data(los_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'}), 'REGION_ID', '033')

# Prepare TPIA Region data
prepare_data(tpia_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'}), 'REGION_ID', '034')

# Load data from SAS tables
sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")
hsp_ind_organization_fact_los_a = sas.sasdata(table='hsp_ind_organization_fact33', libref='hsp_last') 
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los_a.to_df()
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los[hsp_ind_organization_fact_los.FISCAL_YEAR_WH_ID != 17]
display(hsp_ind_organization_fact_los)

hsp_ind_organization_fact_tpia_a = sas.sasdata(table='hsp_ind_organization_fact34', libref='hsp_last') 
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia_a.to_df()
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia.FISCAL_YEAR_WH_ID != 17]
display(hsp_ind_organization_fact_tpia)

# ... (Continuation of your code)
# ... (Previous code)

# Rename columns and apply organization ID mappings for LOS and TPIA
hsp_ind_organization_fact_los['ORGANIZATION_ID'] = hsp_ind_organization_fact_los['ORGANIZATION_ID'].replace({5085: 81180, 5049: 81263})
hsp_ind_organization_fact_tpia['ORGANIZATION_ID'] = hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].replace({5085: 81180, 5049: 81263})
display(hsp_ind_organization_fact_los)
display(hsp_ind_organization_fact_tpia)

# Filter data for specific organization IDs
test_los = hsp_ind_organization_fact_los[hsp_ind_organization_fact_los['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_los)

test_tpia = hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_tpia)

# Define common constants for organization and region
constants = {
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

# Function to prepare data for LOS and TPIA
def prepare_data(df, id_col, indicator_code):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    df['INDICATOR_CODE'] = indicator_code
    for col, value in constants.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_los.columns)
    return df

# Prepare data for LOS and TPIA
los_org_prepared = prepare_data(los_org_com_trd, 'CORP_ID', '033')
tpia_org_prepared = prepare_data(tpia_org_com_trd, 'CORP_ID', '034')

los_reg_prepared = prepare_data(los_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'}), 'REGION_ID', '033')
tpia_reg_prepared = prepare_data(tpia_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'}), 'REGION_ID', '034')

# Concatenate DataFrames for LOS and TPIA
hsp_ind_organization_fact_los_final = pd.concat([hsp_ind_organization_fact_los, los_org_prepared, los_reg_prepared], ignore_index=True)
display(hsp_ind_organization_fact_los_final)

hsp_ind_organization_fact_tpia_final = pd.concat([hsp_ind_organization_fact_tpia, tpia_org_prepared, tpia_reg_prepared], ignore_index=True)
display(hsp_ind_organization_fact_tpia_final)

# ... (Continuation of your code)
# ... (Previous code)

# Define the full range of fiscal years
full_year_range = [18, 19, 20, 21, 22]

# Function to get missing years for a given organization ID
def missing_years(df, org_id):
    existing_years = df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Define a function to create dummy data
def create_dummy_data(df, org_id, indicator_code):
    dummy_data = {column: [] for column in df.columns}
    missing_years_list = missing_years(df, org_id)
    
    for year in missing_years_list:
        for column in df.columns:
            if column == 'ORGANIZATION_ID':
                dummy_data[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_data[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_data[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_data[column].append(indicator_code)
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_data[column].append('999')
            elif column == 'IMPROVEMENT_IND_CODE':
                dummy_data[column].append('999')
            elif column == 'COMPARE_IND_CODE':
                dummy_data[column].append('999')
            elif column == 'DATA_PERIOD_CODE':
                dummy_data[column].append(indicator_code)
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_data[column].append('FY')
            else:
                if df[column].dtype == 'int64' or df[column].dtype == 'float64':
                    dummy_data[column].append(0)  # Default value for numeric columns
                else:
                    dummy_data[column].append('999')  # Default value for string columns
                    
    return pd.DataFrame(dummy_data)

# Identify organizations with less than 5 rows
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_los = counts_los[counts_los < 5].index

# Create dummy data for LOS
dummy_los_data = pd.concat([create_dummy_data(hsp_ind_organization_fact_los_final, org_id, '033') for org_id in orgs_to_add_los], ignore_index=True)
hsp_ind_organization_fact_los_final = pd.concat([hsp_ind_organization_fact_los_final, dummy_los_data], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID and FISCAL_YEAR_WH_ID
hsp_ind_organization_fact_los_final = hsp_ind_organization_fact_los_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_los_final)

# ... (Continuation of your code)

# Identify organizations with less than 5 rows for TPIA
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Create dummy data for TPIA
dummy_tpia_data = pd.concat([create_dummy_data(hsp_ind_organization_fact_tpia_final, org_id, '034') for org_id in orgs_to_add_tpia], ignore_index=True)
hsp_ind_organization_fact_tpia_final = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_tpia_data], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID and FISCAL_YEAR_WH_ID
hsp_ind_organization_fact_tpia_final = hsp_ind_organization_fact_tpia_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_tpia_final)

# ... (Continuation of your code)

# Filtering specific organization IDs
test_los = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_los)

test_tpia = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_tpia)

# ... (Continuation of your code)

or 

# ... (Previous code)

# Define the full range of fiscal years
full_year_range = [18, 19, 20, 21, 22]

# Function to get missing years for a given organization ID
def missing_years(df, org_id):
    existing_years = df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Define a function to create dummy data
def create_dummy_data(df, org_id, indicator_code):
    dummy_data = {column: [] for column in df.columns}
    missing_years_list = missing_years(df, org_id)
    
    for year in missing_years_list:
        for column in df.columns:
            if column == 'ORGANIZATION_ID':
                dummy_data[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_data[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_data[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_data[column].append(indicator_code)
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_data[column].append('999')
            elif column == 'IMPROVEMENT_IND_CODE':
                dummy_data[column].append('999')
            elif column == 'COMPARE_IND_CODE':
                dummy_data[column].append('999')
            elif column == 'DATA_PERIOD_CODE':
                dummy_data[column].append(indicator_code)
            elif column == 'DATA_PERIOD_TYPE_CODE':
                dummy_data[column].append('FY')
            else:
                if df[column].dtype == 'int64' or df[column].dtype == 'float64':
                    dummy_data[column].append(0)  # Default value for numeric columns
                else:
                    dummy_data[column].append('999')  # Default value for string columns
                    
    return pd.DataFrame(dummy_data)

# Identify organizations with less than 5 rows
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_los = counts_los[counts_los < 5].index

# Create dummy data for LOS
dummy_los_data = pd.concat([create_dummy_data(hsp_ind_organization_fact_los_final, org_id, '033') for org_id in orgs_to_add_los], ignore_index=True)
hsp_ind_organization_fact_los_final = pd.concat([hsp_ind_organization_fact_los_final, dummy_los_data], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID and FISCAL_YEAR_WH_ID
hsp_ind_organization_fact_los_final = hsp_ind_organization_fact_los_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_los_final)

# ... (Continuation of your code)

# Identify organizations with less than 5 rows for TPIA
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Create dummy data for TPIA
dummy_tpia_data = pd.concat([create_dummy_data(hsp_ind_organization_fact_tpia_final, org_id, '034') for org_id in orgs_to_add_tpia], ignore_index=True)
hsp_ind_organization_fact_tpia_final = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_tpia_data], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID and FISCAL_YEAR_WH_ID
hsp_ind_organization_fact_tpia_final = hsp_ind_organization_fact_tpia_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
display(hsp_ind_organization_fact_tpia_final)

# ... (Continuation of your code)

# Filtering specific organization IDs
test_los = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_los)

test_tpia = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test_tpia)

# ... ANOHER CODE 

# Data preparation for LOS
los_org_com_trd_av1 = pd.merge(los_corp['CORP_ID'], los_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left')
los_org_com_trd_a = pd.merge(los_org_com_trd_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], los_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']], on=['CORP_ID'], how='left')
merged_df = pd.merge(los_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
los_org_com_trd = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(los_org_com_trd)

# Data preparation for TPIA
tpia_org_com_trd_av1 = pd.merge(tpia_corp['CORP_ID'], tpia_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['CORP_ID'], how='left')
tpia_org_com_trd_a = pd.merge(tpia_org_com_trd_av1[['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], tpia_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']], on=['CORP_ID'], how='left')
merged_df = pd.merge(tpia_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
tpia_org_com_trd = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(tpia_org_com_trd)

# Data preparation for LOS regions
los_reg_a = los_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
los_reg_com_trd_av1 = pd.merge(los_reg_a['REGION_ID'], los_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left')
los_reg_com_trd = pd.merge(los_reg_com_trd_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], los_reg_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']], on=['REGION_ID'], how='left')
display(los_reg_com_trd)

# Data preparation for TPIA regions
tpia_reg_a = tpia_reg.rename(columns={'NEW_REGION_ID': 'REGION_ID'})
tpia_reg_com_trd_av1 = pd.merge(tpia_reg_a['REGION_ID'], tpia_reg_cmp_a[['REGION_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on=['REGION_ID'], how='left')
tpia_reg_com_trd = pd.merge(tpia_reg_com_trd_av1[['REGION_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], tpia_reg_trend_b[['REGION_ID', 'IMPROVEMENT_IND_CODE']], on=['REGION_ID'], how='left')
display(tpia_reg_com_trd)

# Data preparation for LOS and TPIA organizations
sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")

# Fetch data for LOS
hsp_ind_organization_fact_los_a = sas.sasdata(table='hsp_ind_organization_fact33', libref='hsp_last')
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los_a.to_df()
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_los = hsp_ind_organization_fact_los[hsp_ind_organization_fact_los.FISCAL_YEAR_WH_ID != 17]
display(hsp_ind_organization_fact_los)

# Fetch data for TPIA
hsp_ind_organization_fact_tpia_a = sas.sasdata(table='hsp_ind_organization_fact34', libref='hsp_last')
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia_a.to_df()
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia.rename(columns=lambda x: x.upper())
hsp_ind_organization_fact_tpia = hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia.FISCAL_YEAR_WH_ID != 17]
display(hsp_ind_organization_fact_tpia)

# Fetch organization data
sas.saslib('hsp_org', path=r"M:\Groups\eReporting\OurHealthSystem\Lookup Tables")
hsp_organization_ext_a = sas.sasdata(table='hsp_organization_ext', libref='hsp_org')
hsp_organization_ext = hsp_organization_ext_a.to_df()
hsp_organization_ext = hsp_organization_ext.rename(columns=lambda x: x.upper())
display(hsp_organization_ext)

# Update specific ORGANIZATION_IDs in LOS
hsp_ind_organization_fact_los['ORGANIZATION_ID'] = hsp_ind_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81180 if x == 5085 else (81263 if x == 5049 else x))
display(hsp_ind_organization_fact_los)

# Update specific ORGANIZATION_IDs in TPIA
hsp_ind_organization_fact_tpia['ORGANIZATION_ID'] = hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].apply(lambda x: 81180 if x == 5085 else (81263 if x == 5049 else x))
display(hsp_ind_organization_fact_tpia)

# Filter and display specific ORGANIZATION_IDs in LOS
test = hsp_ind_organization_fact_los[hsp_ind_organization_fact_los['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test)

# Filter and display specific ORGANIZATION_IDs in TPIA
test2 = hsp_ind_organization_fact_tpia[hsp_ind_organization_fact_tpia['ORGANIZATION_ID'].isin([81118, 5049, 5085, 81180, 81263])]
display(test2)

# Constant values for org, reg
constant_org_regLOS = {
    "INDICATOR_CODE": "033",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_CODE": "033",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

constant_org_regTPIA = {
    "INDICATOR_CODE": "034",
    "FISCAL_YEAR_WH_ID": 22,
    "SEX_WH_ID": 3,
    "INDICATOR_SUPPRESSION_CODE": '007',
    "TOP_PERFORMER_IND_CODE": '999',
    "DATA_PERIOD_CODE": "034",
    "DATA_PERIOD_TYPE_CODE": 'FY'
}

# Prepare and append data for LOS organizations
los_org_prepared = prepare_org_regLOS(los_org_com_trd, 'CORP_ID')
los_reg_prepared = prepare_org_regLOS(los_reg_com_trd, 'REGION_ID')
hsp_ind_organization_fact_los_final = append_and_sort_data(hsp_ind_organization_fact_los, los_org_prepared, los_reg_prepared)
display(hsp_ind_organization_fact_los_final)

# Prepare and append data for TPIA organizations
tpia_org_prepared = prepare_org_regTPIA(tpia_org_com_trd, 'CORP_ID')
tpia_reg_prepared = prepare_org_regTPIA(tpia_reg_com_trd, 'REGION_ID')
hsp_ind_organization_fact_tpia_final = append_and_sort_data(hsp_ind_organization_fact_tpia, tpia_org_prepared, tpia_reg_prepared)
display(hsp_ind_organization_fact_tpia_final)

# Define the full year range
full_year_range = [18, 19, 20, 21, 22]

# Add missing data for LOS
hsp_ind_organization_fact_los_final = add_missing_data(hsp_ind_organization_fact_los_final, full_year_range)
display(hsp_ind_organization_fact_los_final)

# Add missing data for TPIA
hsp_ind_organization_fact_tpia_final = add_missing_data(hsp_ind_organization_fact_tpia_final, full_year_range)
display(hsp_ind_organization_fact_tpia_final)

# Filter and display specific ORGANIZATION_IDs and REGION_IDs in LOS
test1 = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].isin([693, 2044, 81410, 7020])]
display(test1)

# Filter and display specific ORGANIZATION_IDs and REGION_IDs in TPIA
test2 = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].isin([693, 2044, 81410, 7020])]
display(test2)


# Define a function to prepare DataFrame for LOS and TPIA organizations
def prepare_org_reg(df, id_col, indicator_code):
    df = df.rename(columns={id_col: 'ORGANIZATION_ID', 'PERCENTILE_90': 'INDICATOR_VALUE'})
    constant_values = {
        "INDICATOR_CODE": indicator_code,
        "FISCAL_YEAR_WH_ID": 22,
        "SEX_WH_ID": 3,
        "INDICATOR_SUPPRESSION_CODE": '007',
        "TOP_PERFORMER_IND_CODE": '999',
        "IMPROVEMENT_IND_CODE": '999',
        "COMPARE_IND_CODE": '999',
        "DATA_PERIOD_CODE": indicator_code,
        "DATA_PERIOD_TYPE_CODE": 'FY'
    }
    for col, value in constant_values.items():
        df[col] = value
    df = df.reindex(columns=hsp_ind_organization_fact_los.columns)
    return df

# Prepare data for LOS organizations
los_org_prepared = prepare_org_reg(los_org_com_trd, 'CORP_ID', '033')
los_reg_prepared = prepare_org_reg(los_reg_com_trd, 'REGION_ID', '033')

# Append and sort data for LOS organizations
hsp_ind_organization_fact_los_final = append_and_sort_data(hsp_ind_organization_fact_los, los_org_prepared, los_reg_prepared)
display(hsp_ind_organization_fact_los_final)

# Prepare data for TPIA organizations
tpia_org_prepared = prepare_org_reg(tpia_org_com_trd, 'CORP_ID', '034')
tpia_reg_prepared = prepare_org_reg(tpia_reg_com_trd, 'REGION_ID', '034')

# Append and sort data for TPIA organizations
hsp_ind_organization_fact_tpia_final = append_and_sort_data(hsp_ind_organization_fact_tpia, tpia_org_prepared, tpia_reg_prepared)
display(hsp_ind_organization_fact_tpia_final)

# Define a function to add missing data for given ORGANIZATION_IDs
def add_missing_data(df, org_ids, indicator_code, full_year_range):
    dummy_data = {column: [] for column in df.columns}
    for org_id in org_ids:
        missing_years = [year for year in full_year_range if year not in df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()]
        for year in missing_years:
            for column in df.columns:
                if column == 'ORGANIZATION_ID':
                    dummy_data[column].append(org_id)
                elif column == 'FISCAL_YEAR_WH_ID':
                    dummy_data[column].append(year)
                elif column == 'SEX_WH_ID':
                    dummy_data[column].append(3)
                elif column == 'INDICATOR_CODE':
                    dummy_data[column].append(indicator_code)
                elif column == 'INDICATOR_SUPPRESSION_CODE':
                    dummy_data[column].append('999')
                elif column == 'IMPROVEMENT_IND_CODE':
                    dummy_data[column].append('999')
                elif column == 'COMPARE_IND_CODE':
                    dummy_data[column].append('999')
                elif column == 'DATA_PERIOD_CODE':
                    dummy_data[column].append(indicator_code)
                elif column == 'DATA_PERIOD_TYPE_CODE':
                    dummy_data[column].append('FY')
                else:
                    if df[column].dtype in ['int64', 'float64']:
                        dummy_data[column].append(0)
                    else:
                        dummy_data[column].append('999')
    dummy_df = pd.DataFrame(dummy_data)
    return pd.concat([df, dummy_df], ignore_index=True)

# Define ORGANIZATION_IDs with missing data for LOS and TPIA
org_ids_missing_data_los = [81118, 5049, 5085, 81180, 81263]
org_ids_missing_data_tpia = [693, 2044, 81410, 7020]

# Add missing data for LOS organizations
hsp_ind_organization_fact_los_final = add_missing_data(hsp_ind_organization_fact_los_final, org_ids_missing_data_los, '033', full_year_range)
display(hsp_ind_organization_fact_los_final)

# Add missing data for TPIA organizations
hsp_ind_organization_fact_tpia_final = add_missing_data(hsp_ind_organization_fact_tpia_final, org_ids_missing_data_tpia, '034', full_year_range)
display(hsp_ind_organization_fact_tpia_final)

# Filter and display specific ORGANIZATION_IDs and REGION_IDs in LOS
test1 = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].isin([693, 2044, 81410, 300])]
display(test1)

# Filter and display specific ORGANIZATION_IDs and REGION_IDs in TPIA
test2 = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].isin([693, 2044, 81410, 7020])]
display(test2)


OR USE THIS 

# Define a function to append and sort data for LOS and TPIA organizations
def append_and_sort_data(existing_df, org_df, region_df):
    appended_df = pd.concat([existing_df, org_df, region_df], ignore_index=True)
    sorted_df = appended_df.sort_values(['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
    return sorted_df

# Append and sort data for LOS organizations
hsp_ind_organization_fact_los_final = append_and_sort_data(hsp_ind_organization_fact_los, los_org_prepared, los_reg_prepared)
display(hsp_ind_organization_fact_los_final)

# Append and sort data for TPIA organizations
hsp_ind_organization_fact_tpia_final = append_and_sort_data(hsp_ind_organization_fact_tpia, tpia_org_prepared, tpia_reg_prepared)
display(hsp_ind_organization_fact_tpia_final)

# Define a function to add missing data for given ORGANIZATION_IDs
def add_missing_data(df, org_ids, indicator_code, full_year_range):
    dummy_data = {column: [] for column in df.columns}
    for org_id in org_ids:
        missing_years = [year for year in full_year_range if year not in df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()]
        for year in missing_years:
            for column in df.columns:
                if column == 'ORGANIZATION_ID':
                    dummy_data[column].append(org_id)
                elif column == 'FISCAL_YEAR_WH_ID':
                    dummy_data[column].append(year)
                elif column == 'SEX_WH_ID':
                    dummy_data[column].append(3)
                elif column == 'INDICATOR_CODE':
                    dummy_data[column].append(indicator_code)
                elif column == 'INDICATOR_SUPPRESSION_CODE':
                    dummy_data[column].append('999')
                elif column == 'IMPROVEMENT_IND_CODE':
                    dummy_data[column].append('999')
                elif column == 'COMPARE_IND_CODE':
                    dummy_data[column].append('999')
                elif column == 'DATA_PERIOD_CODE':
                    dummy_data[column].append(indicator_code)
                elif column == 'DATA_PERIOD_TYPE_CODE':
                    dummy_data[column].append('FY')
                else:
                    if df[column].dtype in ['int64', 'float64']:
                        dummy_data[column].append(0)
                    else:
                        dummy_data[column].append('999')
    dummy_df = pd.DataFrame(dummy_data)
    return pd.concat([df, dummy_df], ignore_index=True)

# Define ORGANIZATION_IDs with missing data for LOS and TPIA
org_ids_missing_data_los = [81118, 5049, 5085, 81180, 81263]
org_ids_missing_data_tpia = [693, 2044, 81410, 7020]

# Add missing data for LOS organizations
hsp_ind_organization_fact_los_final = add_missing_data(hsp_ind_organization_fact_los_final, org_ids_missing_data_los, '033', full_year_range)
display(hsp_ind_organization_fact_los_final)

# Add missing data for TPIA organizations
hsp_ind_organization_fact_tpia_final = add_missing_data(hsp_ind_organization_fact_tpia_final, org_ids_missing_data_tpia, '034', full_year_range)
display(hsp_ind_organization_fact_tpia_final)

# Filter and display specific ORGANIZATION_IDs in LOS
org_ids_to_filter_los = [693, 2044, 81410, 300]
filtered_data_los = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].isin(org_ids_to_filter_los)]
display(filtered_data_los)

# Filter and display specific ORGANIZATION_IDs in TPIA
org_ids_to_filter_tpia = [693, 2044, 81410, 7020]
filtered_data_tpia = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].isin(org_ids_to_filter_tpia)]
display(filtered_data_tpia)




MINEEEEEEEEEEEEE FULL TO SUPPRESSION 

import pandas as pd

# Define your data directory path
data_dir = '/path/to/your/data/directory/'

# Load your data into DataFrames (if not already loaded)
hsp_ind_organization_fact33 = pd.read_csv(data_dir + 'hsp_ind_organization_fact33_final.csv')
hsp_ind_organization_fact34 = pd.read_csv(data_dir + 'hsp_ind_organization_fact34_final.csv')

def add_dummy(yr, ind_id):
    # Create DataFrames for organizations with less than five years of data
    less_than_five_yrs_df = hsp_ind_organization_fact33[hsp_ind_organization_fact33.groupby('ORGANIZATION_ID')['FISCAL_YEAR_WH_ID'].transform('count') < 5]

    four_yrs_df = hsp_ind_organization_fact33[hsp_ind_organization_fact33.groupby('ORGANIZATION_ID')['FISCAL_YEAR_WH_ID'].transform('count') == 4]

    new_yr_df = hsp_ind_organization_fact33[hsp_ind_organization_fact33.groupby('ORGANIZATION_ID')['FISCAL_YEAR_WH_ID'].transform('count') == 1]

    # Add dummy data for organizations with less than five years of data
    dummy_data = []
    for organization_id in four_yrs_df['ORGANIZATION_ID'].unique():
        for i in range(1, 5):
            dummy_data.append({
                'ORGANIZATION_ID': organization_id,
                'INDICATOR_CODE': f'0{ind_id}',
                'FISCAL_YEAR_WH_ID': yr - i,
                'SEX_WH_ID': 3,
                'INDICATOR_SUPPRESSION_CODE': '999',
                'TOP_PERFORMER_IND_CODE': '999',
                'IMPROVEMENT_IND_CODE': '999',
                'COMPARE_IND_CODE': '999',
                'INDICATOR_VALUE': None,
                'DATA_PERIOD_CODE': f'0{yr - i}',
                'DATA_PERIOD_TYPE_CODE': 'FY'
            })
    
    # Append the dummy data to the DataFrame
    hsp_ind_organization_fact33 = hsp_ind_organization_fact33.append(pd.DataFrame(dummy_data), ignore_index=True)
    
    # Save the updated DataFrame
    hsp_ind_organization_fact33.to_csv(data_dir + f'hsp_ind_organization_fact{ind_id}_final.csv', index=False)

add_dummy(yr=2024, ind_id=33)  # Add dummy data for LOS indicator (change yr and ind_id as needed)


Now let's implement the apply_suppression macro:

def apply_suppression(yr, ind_id):
    # Define suppression lists for different levels (corp, reg, prov)
    corp_suppression_list = pd.read_csv(data_dir + f'supp_{ind_id}_supp_org_{yr}.csv')
    reg_suppression_list = pd.read_csv(data_dir + f'supp_{ind_id}_supp_reg_{yr}.csv')

    # Create a DataFrame to store all suppression data
    supp_all_level = pd.concat([
        corp_suppression_list.assign(supp_level='Corp'),
        reg_suppression_list.assign(supp_level='Reg')
    ], ignore_index=True)

    # Update suppression codes for corresponding organizations
    hsp_ind_organization_fact33.loc[
        (hsp_ind_organization_fact33['FISCAL_YEAR_WH_ID'] == yr) &
        (hsp_ind_organization_fact33['ORGANIZATION_ID'].isin(supp_all_level['organization_id'])),
        ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE', 'INDICATOR_VALUE']
    ] = '002', '999', '999', '999', None

    # Save the updated DataFrame
    hsp_ind_organization_fact33.to_csv(data_dir + f'hsp_ind_organization_fact{ind_id}_final.csv', index=False)

apply_suppression(yr=2024, ind_id=33)  # Apply suppression for LOS indicator (change yr and ind_id as needed)


def final_submit(ind_id, yr):
    # Update IMPROVEMENT_IND_CODE and COMPARE_IND_CODE for the previous fiscal year
    prev_yr = yr - 1
    hsp_ind_organization_fact33.loc[
        (hsp_ind_organization_fact33['FISCAL_YEAR_WH_ID'] == prev_yr) &
        (hsp_ind_organization_fact33['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003'])),
        'IMPROVEMENT_IND_CODE'
    ] = '999'

    hsp_ind_organization_fact33.loc[
        (hsp_ind_organization_fact33['FISCAL_YEAR_WH_ID'] == prev_yr) &
        (hsp_ind_organization_fact33['COMPARE_IND_CODE'].isin(['001', '002', '003'])),
        'COMPARE_IND_CODE'
    ] = '999'

    # Save the updated DataFrame
    hsp_ind_organization_fact33.to_csv(data_dir + f'hsp_ind_organization_fact{ind_id}_final.csv', index=False)

final_submit(ind_id=33, yr=2024)  # Final submit for LOS indicator (change ind_id and yr as needed)


def check_organization_changes(ind_id):
    # Load the original and updated data
    original_data = pd.read_csv(data_dir + f'hsp_ind_organization_fact{ind_id}.csv')
    updated_data = pd.read_csv(data_dir + f'hsp_ind_organization_fact{ind_id}_final.csv')

    # Identify changes in IMPROVEMENT_IND_CODE and COMPARE_IND_CODE
    changed_orgs = updated_data[
        (updated_data['IMPROVEMENT_IND_CODE'] != original_data['IMPROVEMENT_IND_CODE']) |
        (updated_data['COMPARE_IND_CODE'] != original_data['COMPARE_IND_CODE'])
    ]

    # Print the list of organizations with changes
    print(f"Organizations with changes in IMPROVEMENT_IND_CODE or COMPARE_IND_CODE for indicator {ind_id}:")
    print(changed_orgs[['organization_id', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']])

def check_organization_suppression(ind_id):
    # Load the original and updated data
    original_data = pd.read_csv(data_dir + f'hsp_ind_organization_fact{ind_id}.csv')
    updated_data = pd.read_csv(data_dir + f'hsp_ind_organization_fact{ind_id}_final.csv')

    # Identify organizations with SUPPRESSED status
    suppressed_orgs = updated_data[updated_data['INDICATOR_SUPPRESSION_CODE'] == '001']

    # Print the list of suppressed organizations
    print(f"Suppressed organizations for indicator {ind_id}:")
    print(suppressed_orgs[['organization_id', 'INDICATOR_SUPPRESSION_CODE']])

# Check changes for the LOS indicator
check_organization_changes(ind_id=33)

# Check suppression status for the LOS indicator
check_organization_suppression(ind_id=33)

# Check changes for the TPIA indicator
check_organization_changes(ind_id=34)

# Check suppression status for the TPIA indicator
check_organization_suppression(ind_id=34)



///////////////////////change los to tpia. delete temp 

full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Prepare dummy data
dummy_data = []

# Iterate over ORGANIZATION_IDs with less than 5 rows
for org_id in orgs_to_add_tpia:
    # Get existing years
    existing_years = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    # Find missing years
    missing_years = [year for year in full_year_range if year not in existing_years]

    # Create and append dummy rows
    for year in missing_years:
        dummy_row = {'ORGANIZATION_ID': org_id, 'FISCAL_YEAR_WH_ID': year, 'SEX_WH_ID': 3,
                     'INDICATOR_CODE': '034', 'INDICATOR_SUPPRESSION_CODE': '999',
                     'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999',
                     'DATA_PERIOD_CODE': '034', 'DATA_PERIOD_TYPE_CODE': 'FY'}
        
        # Set default values for other columns
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column not in dummy_row:
                if hsp_ind_organization_fact_tpia_final[column].dtype in ['int64', 'float64']:
                    dummy_row[column] = 0  # Default value for numeric columns
                else:
                    dummy_row[column] = '999'  # Default value for string columns
        
        dummy_data.append(dummy_row)

# Convert dummy data to DataFrame
dummy_tpia = pd.DataFrame(dummy_data)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_tpia_final = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_tpia], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_tpia_34_a = hsp_ind_organization_fact_tpia_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])

# Filter only rows with valid ORGANIZATION_ID
hsp_ind_organization_fact_tpia_34_c = hsp_ind_organization_fact_tpia_34_a[hsp_ind_organization_fact_tpia_34_a['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

# Display the result
display(hsp_ind_organization_fact_tpia_34_c)

# Save the updated DataFrame to a new CSV file, if needed
# hsp_ind_organization_fact_tpia_final.to_csv('updated_data.csv', index=False)


los 

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_los = counts_los[counts_los < 5].index

# Prepare dummy data
dummy_data = []

# Iterate over ORGANIZATION_IDs with less than 5 rows
for org_id in orgs_to_add_los:
    # Get existing years
    existing_years = hsp_ind_organization_fact_los_final[hsp_ind_organization_fact_los_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    # Find missing years
    missing_years = [year for year in full_year_range if year not in existing_years]

    # Create and append dummy rows
    for year in missing_years:
        dummy_row = {'ORGANIZATION_ID': org_id, 'FISCAL_YEAR_WH_ID': year, 'SEX_WH_ID': 3,
                     'INDICATOR_CODE': '033', 'INDICATOR_SUPPRESSION_CODE': '999',
                     'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999',
                     'DATA_PERIOD_CODE': '033', 'DATA_PERIOD_TYPE_CODE': 'FY'}
        
        # Set default values for other columns
        for column in hsp_ind_organization_fact_los_final.columns:
            if column not in dummy_row:
                if hsp_ind_organization_fact_los_final[column].dtype in ['int64', 'float64']:
                    dummy_row[column] = 0  # Default value for numeric columns
                else:
                    dummy_row[column] = '999'  # Default value for string columns
        
        dummy_data.append(dummy_row)

# Convert dummy data to DataFrame
dummy_los = pd.DataFrame(dummy_data)

# Append the dummy data to the original DataFrame
hsp_ind_organization_fact_los_final = pd.concat([hsp_ind_organization_fact_los_final, dummy_los], ignore_index=True)

# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_los_33_a = hsp_ind_organization_fact_los_final.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])

# Filter only rows with valid ORGANIZATION_ID
hsp_ind_organization_fact_los_33_c = hsp_ind_organization_fact_los_33_a[hsp_ind_organization_fact_los_33_a['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

# Display the result
display(hsp_ind_organization_fact_los_33_c)

# Save the updated DataFrame to a new CSV file, if needed
# hsp_ind_organization_fact_los_final.to_csv('updated_data.csv', index=False)

