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


def update_hsp_ind_organization_fact33(df):
    # Update 1
    #TPIA/ELOS Suppress Region and Province due to ED coverage (source from CAD OPS - DQ team);
    #Province  NS(2000), MB(600)
    #Region BC:Interior Health(9054) and Northern Health(9058)NS:Nova Scotia Health Authority Zone 1: Western(80286) and Nova Scotia Health Authority Zone 2: Northern(80287)SK: Far north zone (81404)			 
     #indicator_suppression_code = '901';
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([600, 2000]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    # Update 2
    condition = (
        (df['FISCAL_YEAR_WH_ID'] == 22) &
        (df['ORGANIZATION_ID'].isin([9054, 9058, 80286, 80287, 81404]))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['901', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    # Update 3
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'IMPROVEMENT_IND_CODE'] = '999'

    # Update 4
    condition = (
        (df['FISCAL_YEAR_WH_ID'] < 22) &
        (df['COMPARE_IND_CODE'].isin(['001', '002', '003']))
    )
    df.loc[condition, 'COMPARE_IND_CODE'] = '999'

    #Update 5
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(organization_ids_to_updateLOS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any DQ exists
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_PS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any partial submission apply code 006
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_PS))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['006', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    return df

# Assuming hsp_ind_organization_fact33 is your DataFrame
hsp_ind_organization_fact33 = update_hsp_ind_organization_fact33(hsp_ind_organization_fact_los_33_c)

display(hsp_ind_organization_fact33)


result for fact33 

def update_hsp_ind_organization_fact33(df):
    # Define a helper function to update columns
    def update_columns(df, condition, code, value=None):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 
                           'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = value

    # Update conditions
    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] == 22) & 
                   df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80286, 80287, 81404]), 
                   '901')

    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']), 
                   '999')

    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['COMPARE_IND_CODE'].isin(['001', '002', '003']), 
                   '999')

    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['ORGANIZATION_ID'].isin(organization_ids_to_updateLOS), 
                   '002')

    # Update 6 - DQ and partial submission
    common_condition = (df['FISCAL_YEAR_WH_ID'] < 22) & df['ORGANIZATION_ID'].isin(filtered_ID_PS)
    update_columns(df, common_condition, '002')
    update_columns(df, common_condition, '006')

    return df

# Applying the function
hsp_ind_organization_fact33 = update_hsp_ind_organization_fact33(hsp_ind_organization_fact_los_33_c)
display(hsp_ind_organization_fact33)

