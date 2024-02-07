# Visions_Systems_OpenCV
# Define a function to filter and create DataFrame
///
CONVERTED

import pandas as pd
import numpy as np

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Assuming the DataFrame is already defined
hsp_ind_organization_fact_los_final = pd.DataFrame()

# Function to determine missing years for a given ORGANIZATION_ID
def missing_years_los(org_id, df, year_range):
    existing_years = df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return set(year_range) - set(existing_years)

# Function to generate dummy data for each missing year
def generate_dummy_data(org_id, missing_years, columns, default_values):
    dummy_data = []
    for year in missing_years:
        row = {col: default_values.get(col, np.nan) for col in columns}
        row['ORGANIZATION_ID'] = org_id
        row['FISCAL_YEAR_WH_ID'] = year
        dummy_data.append(row)
    return dummy_data

# Default values for dummy rows
default_values = {
    'SEX_WH_ID': 3, 'INDICATOR_CODE': '033', 'INDICATOR_SUPPRESSION_CODE': '999',
    'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999', 'DATA_PERIOD_TYPE_CODE': 'FY',
    'INDICATOR_VALUE': np.nan
}

# Identify organizations with less than 5 rows
counts_los = hsp_ind_organization_fact_los_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_los = counts_los[counts_los < 5].index

# Generate and append dummy and blank rows
all_dummy_rows_los = []
for org_id in orgs_to_add_los:
    missing_years = missing_years_los(org_id, hsp_ind_organization_fact_los_final, full_year_range)
    dummy_rows = generate_dummy_data(org_id, missing_years, hsp_ind_organization_fact_los_final.columns, default_values)
    all_dummy_rows_los.extend(dummy_rows)

# Handle specific ORGANIZATION_IDs for blank rows
additional_org_ids_los = [7028]
for org_id in additional_org_ids_los:
    missing_years = missing_years_los(org_id, hsp_ind_organization_fact_los_final, full_year_range)
    blank_rows = generate_dummy_data(org_id, missing_years, hsp_ind_organization_fact_los_final.columns, default_values)
    all_dummy_rows_los.extend(blank_rows)

# Convert to DataFrame and append to the original DataFrame
dummy_and_blank_rows_los = pd.DataFrame(all_dummy_rows_los)
hsp_ind_organization_fact_los_33_a = pd.concat([hsp_ind_organization_fact_los_final, dummy_and_blank_rows_los], ignore_index=True)

# Optionally, sort and filter the DataFrame
hsp_organization_ext = pd.DataFrame()  # Assuming this DataFrame is already defined
hsp_ind_organization_fact_los_33_b = hsp_ind_organization_fact_los_33_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
final_df_los = hsp_ind_organization_fact_los_33_b[hsp_ind_organization_fact_los_33_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

# Validate the final DataFrame
# Check for missing values in critical columns
missing_values_check_los = final_df_los.isnull().sum()
print("Missing values in each column (LOS DataFrame):\n", missing_values_check_los)

# Verify the shape of the final DataFrame
print("Shape of the final DataFrame (LOS DataFrame):", final_df_los.shape)

# Display a sample of the final DataFrame for review
print("Sample data from the final DataFrame (LOS DataFrame):")
print(final_df_los.head())

////
TPIA
import pandas as pd
import numpy as np

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Assuming the DataFrame is already defined
hsp_ind_organization_fact_tpia_final = pd.DataFrame()

# Function to determine missing years for a given ORGANIZATION_ID
def missing_years_tpia(org_id, df, year_range):
    existing_years = df[df['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return set(year_range) - set(existing_years)

# Function to generate dummy data for each missing year
def generate_dummy_data(org_id, missing_years, columns, default_values):
    dummy_data = []
    for year in missing_years:
        row = {col: default_values.get(col, np.nan) for col in columns}
        row['ORGANIZATION_ID'] = org_id
        row['FISCAL_YEAR_WH_ID'] = year
        dummy_data.append(row)
    return dummy_data

# Default values for dummy rows
default_values = {
    'SEX_WH_ID': 3, 'INDICATOR_CODE': '033', 'INDICATOR_SUPPRESSION_CODE': '999',
    'IMPROVEMENT_IND_CODE': '999', 'COMPARE_IND_CODE': '999', 'DATA_PERIOD_TYPE_CODE': 'FY',
    'INDICATOR_VALUE': np.nan
}

# Identify organizations with less than 5 rows
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Generate and append dummy and blank rows
all_dummy_rows = []
for org_id in orgs_to_add_tpia:
    missing_years = missing_years_tpia(org_id, hsp_ind_organization_fact_tpia_final, full_year_range)
    dummy_rows = generate_dummy_data(org_id, missing_years, hsp_ind_organization_fact_tpia_final.columns, default_values)
    all_dummy_rows.extend(dummy_rows)

# Handle specific ORGANIZATION_IDs for blank rows
additional_org_ids = [7028]
for org_id in additional_org_ids:
    missing_years = missing_years_tpia(org_id, hsp_ind_organization_fact_tpia_final, full_year_range)
    blank_rows = generate_dummy_data(org_id, missing_years, hsp_ind_organization_fact_tpia_final.columns, default_values)
    all_dummy_rows.extend(blank_rows)

# Convert to DataFrame and append to the original DataFrame
dummy_and_blank_rows = pd.DataFrame(all_dummy_rows)
hsp_ind_organization_fact_tpia_34_a = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_and_blank_rows], ignore_index=True)

# Optionally, sort and filter the DataFrame
hsp_organization_ext = pd.DataFrame()  # Assuming this DataFrame is already defined
hsp_ind_organization_fact_tpia_34_b = hsp_ind_organization_fact_tpia_34_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
final_df = hsp_ind_organization_fact_tpia_34_b[hsp_ind_organization_fact_tpia_34_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

# Finalizing and validating the output

# Convert to DataFrame and append to the original DataFrame
dummy_and_blank_rows = pd.DataFrame(all_dummy_rows)
hsp_ind_organization_fact_tpia_34_a = pd.concat([hsp_ind_organization_fact_tpia_final, dummy_and_blank_rows], ignore_index=True)

# Optionally, sort and filter the DataFrame
hsp_organization_ext = pd.DataFrame()  # Assuming this DataFrame is already defined
hsp_ind_organization_fact_tpia_34_b = hsp_ind_organization_fact_tpia_34_a.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
final_df = hsp_ind_organization_fact_tpia_34_b[hsp_ind_organization_fact_tpia_34_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

# Validate the final DataFrame
# Check for missing values in critical columns
missing_values_check = final_df.isnull().sum()
print("Missing values in each column:\n", missing_values_check)

# Verify the shape of the final DataFrame
print("Shape of the final DataFrame:", final_df.shape)

# Display a sample of the final DataFrame for review
print("Sample data from the final DataFrame:")
print(final_df.head())
//////////////
BLENDED
///////



//////////////////////
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
            elif column =='INDICATOR_VALUE':
                dummy_datalos[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                dummy_datalos[column].append(f'0{year}')
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

# Now create a list of orgs that are parial submissions, check if they already exist in the dataset, if not add blank rows for these org IDs. 7028 was added because of some reason

add_blank={'ORGANIZATION_ID' : [7028]}
add_blank_los=pd.DataFrame(add_blank)

blank_los = {column: [] for column in hsp_ind_organization_fact_los_final.columns}
# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in add_blank_los['ORGANIZATION_ID']:
    missing_years = missing_years_los(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_los_final.columns:
            if column == 'ORGANIZATION_ID':
                blank_los[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                blank_los[column].append(year)
            elif column == 'SEX_WH_ID':
                blank_los[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                blank_los[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                blank_los[column].append('006')
            elif column=='IMPROVEMENT_IND_CODE':
                blank_los[column].append('999')
            elif column =='COMPARE_IND_CODE':
                blank_los[column].append('999')
            elif column =='INDICATOR_VALUE':
                blank_los[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                blank_los[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                blank_los[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_los_final[column].dtype == 'int64' or hsp_ind_organization_fact_los_final[column].dtype == 'float64':
                        blank_los[column].append(np.nan)  # Default value for numeric columns
                    else:
                        blank_los[column].append('999')  # Default value for string columns

# Convert blank_los data to DataFrame
blank_los_add = pd.DataFrame(blank_los)

#Append the blank data to the original DataFrame
hsp_ind_organization_fact_los_33_b = pd.concat([hsp_ind_organization_fact_los_33_a, blank_los_add], ignore_index=True)


# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_los_33_c = hsp_ind_organization_fact_los_33_b.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_los_33_d = hsp_ind_organization_fact_los_33_c[hsp_ind_organization_fact_los_33_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]

///

TPIA

# Define the full range of years
full_year_range = [18, 19, 20, 21, 22]

# Count the number of rows for each ORGANIZATION_ID
counts_tpia = hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'].value_counts()

# Find ORGANIZATION_IDs with less than 5 rows
orgs_to_add_tpia = counts_tpia[counts_tpia < 5].index

# Define a function to get missing years
def missing_years_tpia(org_id):
    existing_years = hsp_ind_organization_fact_tpia_final[hsp_ind_organization_fact_tpia_final['ORGANIZATION_ID'] == org_id]['FISCAL_YEAR_WH_ID'].unique()
    return [year for year in full_year_range if year not in existing_years]

# Prepare dummy data
dummy_datatpia = {column: [] for column in hsp_ind_organization_fact_tpia_final.columns}


# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in orgs_to_add_tpia:
    missing_years = missing_years_tpia(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column == 'ORGANIZATION_ID':
                dummy_datatpia[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                dummy_datatpia[column].append(year)
            elif column == 'SEX_WH_ID':
                dummy_datatpia[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                dummy_datatpia[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                dummy_datatpia[column].append('999')
            elif column=='IMPROVEMENT_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='COMPARE_IND_CODE':
                dummy_datatpia[column].append('999')
            elif column =='INDICATOR_VALUE':
                dummy_datatpia[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                dummy_datatpia[column].append(f'0{year}')
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

# Now create a list of orgs that are parial submissions, check if they already exist in the dataset, if not add blank rows for these org IDs. 7028 was added because of some reason

add_blank={'ORGANIZATION_ID' : [7028]}
add_blank_tpia=pd.DataFrame(add_blank)

blank_tpia = {column: [] for column in hsp_ind_organization_fact_tpia_final.columns}
# Add dummy rows for each ORGANIZATION_ID with less than 5 rows
for org_id in add_blank_tpia['ORGANIZATION_ID']:
    missing_years = missing_years_tpia(org_id)
    for year in missing_years:
        for column in hsp_ind_organization_fact_tpia_final.columns:
            if column == 'ORGANIZATION_ID':
                blank_tpia[column].append(org_id)
            elif column == 'FISCAL_YEAR_WH_ID':
                blank_tpia[column].append(year)
            elif column == 'SEX_WH_ID':
                blank_tpia[column].append(3)  # Example: numeric column
            elif column == 'INDICATOR_CODE':
                blank_tpia[column].append('033')
            elif column == 'INDICATOR_SUPPRESSION_CODE':
                blank_tpia[column].append('006')
            elif column=='IMPROVEMENT_IND_CODE':
                blank_tpia[column].append('999')
            elif column =='COMPARE_IND_CODE':
                blank_tpia[column].append('999')
            elif column =='INDICATOR_VALUE':
                blank_tpia[column].append(np.nan)
            elif column =='DATA_PERIOD_CODE':
                blank_tpia[column].append(f'0{year}')
            elif column == 'DATA_PERIOD_TYPE_CODE':
                blank_tpia[column].append('FY')    
            else:   
                    if hsp_ind_organization_fact_tpia_final[column].dtype == 'int64' or hsp_ind_organization_fact_tpia_final[column].dtype == 'float64':
                        blank_tpia[column].append(np.nan)  # Default value for numeric columns
                    else:
                        blank_tpia[column].append('999')  # Default value for string columns

# Convert blank_tpia data to DataFrame
blank_tpia_add = pd.DataFrame(blank_tpia)

#Append the blank data to the original DataFrame
hsp_ind_organization_fact_tpia_34_b = pd.concat([hsp_ind_organization_fact_tpia_34_a, blank_tpia_add], ignore_index=True)


# Optionally, sort the DataFrame based on ORGANIZATION_ID or any other column
hsp_ind_organization_fact_tpia_34_c = hsp_ind_organization_fact_tpia_34_b.sort_values(by=['ORGANIZATION_ID', 'FISCAL_YEAR_WH_ID'])
hsp_ind_organization_fact_tpia_34_d = hsp_ind_organization_fact_tpia_34_c[hsp_ind_organization_fact_tpia_34_b['ORGANIZATION_ID'].isin(hsp_organization_ext['ORGANIZATION_ID'])]








///////////////////////////////////////////////////////////////////////////////////////



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


TPIA RABS 

def update_hsp_ind_organization_fact34(df):
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
       (df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA))
    )
    df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = ['002', '999', '999', '999']
    df.loc[condition, 'INDICATOR_VALUE'] = None

    #Update 6 -if any DQ exists
    condition = (
       (df['FISCAL_YEAR_WH_ID'] < 22) &
       (df['ORGANIZATION_ID'].isin(filtered_ID_DQ_TPIA))
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
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_c)

display(hsp_ind_organization_fact34)



blended 



You can blend these two functions together into a single function by creating a common function that takes the DataFrame, fiscal year conditions, organization ID conditions, and suppression code mappings as arguments. Here's how you can do it:

```python
def update_hsp_ind_organization_fact(df, fiscal_year_conditions, org_id_conditions, suppression_code):
    def update_columns(df, condition, code, value=None):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 
                           'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = value

    # Update conditions
    update_columns(df, fiscal_year_conditions, suppression_code)

    # Update 6 - DQ and partial submission
    common_condition = (df['FISCAL_YEAR_WH_ID'] < 22) & df['ORGANIZATION_ID'].isin(filtered_ID_PS)
    update_columns(df, common_condition, '002')
    update_columns(df, common_condition, '006')

    return df

# Create the list of organization IDs for each function
organization_ids_to_updateLOS = [600, 2000, 9054, 9058, 80286, 80287, 81404]
organization_ids_to_updateTPIA = organization_ids_to_updateLOS  # Assuming they are the same
filtered_ID_PS = [list of organization IDs]  # Define the list of organization IDs for partial submission
filtered_ID_DQ_TPIA = [list of organization IDs]  # Define the list of organization IDs for DQ and TPIA

# Define fiscal year conditions
fiscal_year_condition33 = (hsp_ind_organization_fact_los_33_c['FISCAL_YEAR_WH_ID'] == 22)
fiscal_year_condition34 = (hsp_ind_organization_fact_tpia_34_c['FISCAL_YEAR_WH_ID'] == 22)

# Apply the function for fact33 and fact34 separately
hsp_ind_organization_fact33 = update_hsp_ind_organization_fact(
    hsp_ind_organization_fact_los_33_c, fiscal_year_condition33, organization_ids_to_updateLOS, '901'
)

hsp_ind_organization_fact34 = update_hsp_ind_organization_fact(
    hsp_ind_organization_fact_tpia_34_c, fiscal_year_condition34, organization_ids_to_updateTPIA, '901'
)

# Display the updated DataFrames
display(hsp_ind_organization_fact33)
display(hsp_ind_organization_fact34)
```

This code defines a single function `update_hsp_ind_organization_fact` that can be applied to both fact33 and fact34 DataFrames by passing the appropriate fiscal year conditions, organization ID conditions, and suppression code. It also assumes that `organization_ids_to_updateTPIA` and `filtered_ID_PS` are the same for both functions, but you can adjust them as needed.


JUST TPIA 

def update_hsp_ind_organization_fact34(df):
    def update_columns(df, condition, suppression_code, value=None):
        df.loc[condition, ['INDICATOR_SUPPRESSION_CODE', 'TOP_PERFORMER_IND_CODE', 
                           'IMPROVEMENT_IND_CODE', 'COMPARE_IND_CODE']] = [suppression_code, '999', '999', '999']
        df.loc[condition, 'INDICATOR_VALUE'] = value

    # Update 1 and 2
    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] == 22) & 
                   df['ORGANIZATION_ID'].isin([600, 2000, 9054, 9058, 80286, 80287, 81404]), 
                   '901')

    # Update 3 and 4
    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['IMPROVEMENT_IND_CODE'].isin(['001', '002', '003']), 
                   '999')

    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['COMPARE_IND_CODE'].isin(['001', '002', '003']), 
                   '999')

    # Update 5
    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['ORGANIZATION_ID'].isin(organization_ids_to_updateTPIA), 
                   '002')

    # Update 6 - DQ and partial submission
    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['ORGANIZATION_ID'].isin(filtered_ID_DQ_TPIA), 
                   '002')

    update_columns(df, 
                   (df['FISCAL_YEAR_WH_ID'] < 22) & 
                   df['ORGANIZATION_ID'].isin(filtered_ID_PS), 
                   '006')

    return df

# Applying the function
hsp_ind_organization_fact34 = update_hsp_ind_organization_fact34(hsp_ind_organization_fact_tpia_34_c)
display(hsp_ind_organization_fact34)

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Bron 

.. I am trying to access 5 years of NACRS data/create 5 dataframes, from these SAS folders:

J:\DataHoldings\PROD\NACRS\2018\UNRESTRICTED
J:\DataHoldings\PROD\NACRS\2019\UNRESTRICTED
J:\DataHoldings\PROD\NACRS\2020\UNRESTRICTED
J:\DataHoldings\PROD\NACRS\2021\UNRESTRICTED
J:\DataHoldings\PROD\NACRS\2022\UNRESTRICTED

I can easily do one year at a time, like this (example = 2021):

# 1- establishing a saspy sessionusing your CIHI credential. (You need to have access to the SAS server!)
sas = saspy.SASsession(cfgname='cihiprod', results='HTML')
# Define a saspy library with the location of the folder of the SAS dataset on sas server.
sas.saslib('nacrslib', path=r"J:\DataHoldings\PROD\NACRS")


yr = str(21)

sas.saslib('nacrs' + yr, path=r"J:\DataHoldings\PROD\NACRS\2021\UNRESTRICTED")
                               
# filtering required data elements and subsetting ED records from AMBULATORY_CARE table: 
options = {
           'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE', # filter by name of the requiured columns
            'where' : 'AMCARE_GROUP_CODE = "ED" ' # filter by value of the rows in the selected columns
           }

filtered_NACRS_yr = sas.sasdata(table='ambulatory_care', libref='nacrs' + yr, dsopts=options)
#select tables, set conditions and limited columns here


#Return a sas data object.
#table parameter: just put the name of the SAS dataset, don't put the sas7bdat extention)
vars()["filtered_NACRS" + yr + "_AMCARE"] = sas.sasdata(table='AMBULATORY_CARE', libref='nacrs' + yr, dsopts=options)  #8.54 GB SAS dataset

# Convert to dataframe
vars()["df_filtered_NACRS" + yr + "_AMCARE"] = vars()["filtered_NACRS" + yr + "_AMCARE"].to_df()

# Convert float to int
vars()["df_filtered_NACRS" + yr + "_AMCARE"]['AM_CARE_KEY'] = vars()["df_filtered_NACRS" + yr + "_AMCARE"]['AM_CARE_KEY'].astype(int)

vars()["df_filtered_NACRS" + yr + "_AMCARE"].info()

And this gives me a dataframe called df_filtered_NACRS21_AMCARE.  Ideally, I would like to write all this into a function that creates 5 dataframes, one for each fiscal year. 

Most recently I have tried this, just for the path part and the column selections – I figured that was easiest to start with – I could be wrong!

I tried this:

def my_read(yr_list, options):
    result_dict={}
    for yr in my_yr_ls:
        # static path with yr embedded into it:  
       
        path= 'r"J:\DataHoldings\PROD\NACRS\”+ str(yr) +” \UNRESTRICTED"' 


        sas.saslib('nacrs20' + str(yr), path, options)

# filtering required data elements and subsetting ED records from AMBULATORY_CARE table: 
result_dict[yr] = sas.sasdata(table='ambulatory_care', libref='nacrs' + str(yr), dsopts=options)
    
return result_dict

# setting the parameters:
my_yr_ls = [18,19,20,21,22]
my_options = {
           'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE', # filter by name of the required columns
            'where' : 'AMCARE_GROUP_CODE = "ED" ' # filter by value of the rows in the selected columns

         }

#call the function:
my_read(yr_list, options= my_options)

And I get this message:

SyntaxError: (unicode error) 'unicodeescape' codec can't decode bytes in position 22-23: malformed \N character escape

So I tried a solution like using double slashes to escape the escape:

def my_read(yr_list, options):
    result_dict={}
    for yr in my_yr_ls:
        # static path with yr embedded into it:  
       
        path= 'r"J:\\DataHoldings\\PROD\\NACRS\\”+ str(yr) +” \\UNRESTRICTED"' 


        sas.saslib('nacrs20' + str(yr), path, options)

# filtering required data elements and subsetting ED records from AMBULATORY_CARE table: 
result_dict[yr] = sas.sasdata(table='ambulatory_care', libref='nacrs' + str(yr), dsopts=options)
    
return result_dict

# setting the parameters:
my_yr_ls = [18,19,20,21,22]
my_options = {
           'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE', # filter by name of the required columns
            'where' : 'AMCARE_GROUP_CODE = "ED" ' # filter by value of the rows in the selected columns

         }

#call the function:
my_read(yr_list, options= my_options)

And now I get:

NameError: name 'result_dict' is not defined

So I seem to either get an error about the path, or about not defining something. 



//////////////////////////////////////////

trying to see if this is a working code 


import saspy
import pandas as pd

def read_nacrs_data(years):
    data_frames = {}  # Dictionary to store dataframes for each year
    
    # Create a SAS session (you need to have the proper SAS configuration)
    sas = saspy.SASsession(cfgname='cihiprod', results='HTML')
    
    for year in years:
        libref = f'nacrs20{year}'  # Define the SAS library reference
        
        # Construct the path for each year's data
        path = f"J:\\DataHoldings\\PROD\\NACRS\\{year}\\UNRESTRICTED"
        
        # Define options for data filtering if needed
        options = {
            'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE',
            'where': 'AMCARE_GROUP_CODE = "ED"'
        }
        
        # Create a SAS library
        sas.saslib(libref, path, options)
        
        # Read data from the SAS library and store it in a dataframe
        data_frames[year] = sas.sasdata(table='ambulatory_care', libref=libref, dsopts=options).to_df()
    
    sas.disconnect()  # Disconnect the SAS session when done
    return data_frames

# List of years for which you want to access data
years_to_access = [2018, 2019, 2020, 2021, 2022]

# Call the function to read NACRS data for the specified years
nacrs_data = read_nacrs_data(years_to_access)

# Now, you have a dictionary 'nacrs_data' where each key represents a year, and the corresponding value is a dataframe containing the data for that year.
# Access the dataframe for 2018
df_2018 = nacrs_data[2018]

# Filter rows where AMCARE_GROUP_CODE is "ED"
ed_data_2018 = df_2018[df_2018['AMCARE_GROUP_CODE'] == 'ED']

# Select specific columns
selected_columns = ed_data_2018[['AM_CARE_KEY', 'FACILITY_AM_CARE_NUM', 'SUBMISSION_FISCAL_YEAR']]
# Combine data from all years into a single dataframe
combined_data = pd.concat([nacrs_data[year] for year in years_to_access], ignore_index=True)
# Calculate summary statistics for a specific column
mean_am_care_key = combined_data['AM_CARE_KEY'].mean()

# Plot data using libraries like Matplotlib or Seaborn
import matplotlib.pyplot as plt
combined_data['SUBMISSION_FISCAL_YEAR'].value_counts().plot(kind='bar')
plt.title('Counts by Fiscal Year')
plt.xlabel('Fiscal Year')
plt.ylabel('Count')
plt.show()




or 



import saspy
import pandas as pd

def read_nacrs_data(years):
    data_frames = {}  # Dictionary to store dataframes for each year
    
    # Create a SAS session (you need to have the proper SAS configuration)
    sas = saspy.SASsession(cfgname='cihiprod', results='HTML')
    
    for year in years:
        libref = f'nacrs20{year}'  # Define the SAS library reference
        
        # Construct the path for each year's data
        path = f"J:\\DataHoldings\\PROD\\NACRS\\{year}\\UNRESTRICTED"
        
        # Define options for data filtering if needed
        options = {
            'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE',
            'where': 'AMCARE_GROUP_CODE = "ED"'
        }
        
        # Create a SAS library
        sas.saslib(libref, path, options)
        
        # Read data from the SAS library and store it in a dataframe
        data_frames[year] = sas.sasdata(table='ambulatory_care', libref=libref, dsopts=options).to_df()
    
    sas.disconnect()  # Disconnect the SAS session when done
    return data_frames

# List of years for which you want to access data
years_to_access = [2018, 2019, 2020, 2021, 2022]

# Call the function to read NACRS data for the specified years
nacrs_data = read_nacrs_data(years_to_access)

# Now, you have a dictionary 'nacrs_data' where each key represents a year, and the corresponding value is a dataframe containing the data for that year.

# Example: Access the dataframe for 2018
df_2018 = nacrs_data[2018]




or 



import saspy
import pandas as pd

def read_nacrs_data(years):
    data_frames = {}  # Dictionary to store dataframes for each year
    
    # Create a SAS session (you need to have the proper SAS configuration)
    sas = saspy.SASsession(cfgname='cihiprod', results='HTML')
    
    for year in years:
        libref = f'nacrs20{year}'  # Define the SAS library reference
        
        # Construct the path for each year's data
        path = f"J:\\DataHoldings\\PROD\\NACRS\\{year}\\UNRESTRICTED"
        
        # Define options for data filtering if needed
        options = {
            'keep': 'AM_CARE_KEY FACILITY_AM_CARE_NUM SUBMISSION_FISCAL_YEAR SUBMISSION_PERIOD AMCARE_GROUP_CODE',
            'where': 'AMCARE_GROUP_CODE = "ED"'
        }
        
        # Create a SAS library
        sas.saslib(libref, path, options)
        
        # Read data from the SAS library and store it in a dataframe
        data_frames[year] = sas.sasdata(table='ambulatory_care', libref=libref, dsopts=options).to_df()
    
    sas.disconnect()  # Disconnect the SAS session when done
    return data_frames

# List of years for which you want to access data
years_to_access = [2018, 2019, 2020, 2021, 2022]

# Call the function to read NACRS data for the specified years
nacrs_data = read_nacrs_data(years_to_access)

# Now, you have a dictionary 'nacrs_data' where each key represents a year, and the corresponding value is a dataframe containing the data for that year.

# Example: Access the dataframe for 2018
df_2018 = nacrs_data[2018]



/// mine 

# Create a new dataframe tpia_org_22_a
tpia_org_22_a = pd.DataFrame()  # Initialize an empty dataframe or create it using your data source

# Merge tpia_org_22 into tpia_org_22_a
tpia_org_22_a = pd.merge(tpia_org_22_a, tpia_org_22[['CORP_ID']], on='CORP_ID', how='left', indicator=True)

# Filter for rows where the merge indicator is 'left_only'
tpia_org_22_a = tpia_org_22_a[tpia_org_22_a['_merge'] == 'left_only'].drop(columns=['_merge'])

# Rename the 'SUBMISSION_FISCAL_YEAR' column to 'FISCAL_YEAR'
tpia_org_22_a = tpia_org_22_a.rename(columns={'SUBMISSION_FISCAL_YEAR': 'FISCAL_YEAR'})

# Replace specific values in the 'CORP_ID' column
tpia_org_22_a['CORP_ID'].replace({5085: 81180, 5049: 81263}, inplace=True)



roundd


df['column_name'] = df['column_name'].apply(lambda x: round(x - 0.05, 1))


jjjj

import pandas as pd
RABS ROUND ERROR 

test3=hsp_ind_organization_fact33.round\\(['INDICATOR_VALUE'], 1)
def custom_round(value):
    str_value = str(value)
    decimal_index = str_value.find('.')
    if decimal_index != -1 and decimal_index + 1 < len(str_value):
        digit_after_decimal = int(str_value[decimal_index + 1])
        if digit_after_decimal >= 5:
            return round(value, 1)
    return value




correct of round for rabs delette after 
import pandas as pd

# Example DataFrame
# hsp_ind_organization_fact33 = pd.DataFrame({'INDICATOR_VALUE': [your_data_here]})

# Round the 'INDICATOR_VALUE' column to one decimal place
hsp_ind_organization_fact33['INDICATOR_VALUE'] = hsp_ind_organization_fact33['INDICATOR_VALUE'].round(1)

# Display the DataFrame
display(hsp_ind_organization_fact33)
///

import pandas as pd

# Assuming hsp_ind_organization_fact33 is your DataFrame
# Example: hsp_ind_organization_fact33 = pd.DataFrame({'INDICATOR_VALUE': [your_data_here]})

def custom_round(value):
    str_value = str(value)
    decimal_index = str_value.find('.')
    if decimal_index != -1 and decimal_index + 1 < len(str_value):
        digit_after_decimal = int(str_value[decimal_index + 1])
        if digit_after_decimal >= 5:
            return round(value, 1)
    return value

# Apply custom_round to the 'INDICATOR_VALUE' column
hsp_ind_organization_fact33['INDICATOR_VALUE'] = hsp_ind_organization_fact33['INDICATOR_VALUE'].apply(custom_round)

# Display the DataFrame
display(hsp_ind_organization_fact33)

