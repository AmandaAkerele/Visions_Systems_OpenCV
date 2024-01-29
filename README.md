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


/////

los_org_com_trd_av1=pd.merge(los_corp['CORP_ID'], los_org_cmp_a[['CORP_ID','COMPARE_IND_CODE','PERCENTILE_90']],on=['CORP_ID'],how='left')
los_org_com_trd_a=pd.merge(los_org_com_trd_av1[['CORP_ID','PERCENTILE_90','COMPARE_IND_CODE']], los_org_trend_b[['CORP_ID','IMPROVEMENT_IND_CODE']],on=['CORP_ID'],how='left')
display(los_org_com_trd_a)

merged_df=pd.merge(los_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
los_org_com_trd=merged_df[merged_df['_merge']=='left_only']
los_org_com_trd=los_org_com_trd.drop(columns=['_merge','SUBMISSION_FISCAL_YEAR','NACRS_ED_FLG'])
display(los_org_com_trd)

sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")
hsp_ind_organization_fact_los_a = sas.sasdata(table='hsp_ind_organization_fact33', libref='hsp_last') 
hsp_ind_organization_fact_los=hsp_ind_organization_fact_los_a.to_df()
hsp_ind_organization_fact_los=hsp_ind_organization_fact_los.rename(columns=lambda x: x.upper())
display(hsp_ind_organization_fact_los)

hsp_ind_organization_fact_los=hsp_ind_organization_fact_los[hsp_ind_organization_fact_los.FISCAL_YEAR_WH_ID !=17]
display(hsp_ind_organization_fact_los)

hsp_ind_organization_fact_los['ORGANIZATION_ID']=hsp_ind_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81180 if x==5085 else x)
hsp_ind_organization_fact_los['ORGANIZATION_ID']=hsp_ind_organization_fact_los['ORGANIZATION_ID'].apply(lambda x: 81263 if x==5049 else x)
display(hsp_ind_organization_fact_los)

test=hsp_ind_organization_fact_los[hsp_ind_organization_fact_los['ORGANIZATION_ID'].isin([81118,5049,5085])]
display(test)


python rab 
import pandas as pd

# Merge DataFrames and display los_org_com_trd_a
los_org_com_trd_a = pd.merge(
    pd.merge(los_corp[['CORP_ID']], los_org_cmp_a[['CORP_ID', 'COMPARE_IND_CODE', 'PERCENTILE_90']], on='CORP_ID', how='left')[
        ['CORP_ID', 'PERCENTILE_90', 'COMPARE_IND_CODE']], los_org_trend_b[['CORP_ID', 'IMPROVEMENT_IND_CODE']],
    on='CORP_ID', how='left')
display(los_org_com_trd_a)

# Merge DataFrames and filter rows
merged_df = pd.merge(los_org_com_trd_a, ed_nacrs_flg_1, on='CORP_ID', how='left', indicator=True)
los_org_com_trd = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge', 'SUBMISSION_FISCAL_YEAR', 'NACRS_ED_FLG'])
display(los_org_com_trd)

# Load the SAS dataset and process it
sas.saslib('hsp_last', path=r"M:\Groups\eReporting\OurHealthSystem\Data Submission\17. Archive - Fall 2022")
hsp_ind_organization_fact_los = sas.sasdata(table='hsp_ind_organization_fact33', libref='hsp_last').to_df() \
    .rename(columns=str.upper)[lambda x: x['FISCAL_YEAR_WH_ID'] != 17]

# Replace values in the 'ORGANIZATION_ID' column
hsp_ind_organization_fact_los['ORGANIZATION_ID'] = hsp_ind_organization_fact_los['ORGANIZATION_ID'].replace(5085, 81180)

# Display the final hsp_ind_organization_fact_los DataFrame
display(hsp_ind_organization_fact_los)

