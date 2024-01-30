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



