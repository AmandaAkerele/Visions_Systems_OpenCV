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




//////
import pandas as pd

# Define file paths for data
matched_los_reg_22_path = "matched_los_reg_22.csv"
matched_tpia_reg_22_path = "matched_tpia_reg_22.csv"
supp_los_supp_reg_22_path = "supp_los_supp_reg_22.csv"
supp_tpia_supp_reg_22_path = "supp_tpia_supp_reg_22.csv"
ed_facility_org_22_path = "ed_facility_org_22.csv"
com_tpia_org_cmp_a_path = "com_tpia_org_cmp_a.csv"
com_los_org_cmp_a_path = "com_los_org_cmp_a.csv"
com_tpia_reg_cmp_a_path = "com_tpia_reg_cmp_a.csv"
com_los_reg_cmp_a_path = "com_los_reg_cmp_a.csv"
com_tpia_prov_cmp_a_path = "com_tpia_prov_cmp_a.csv"
com_los_prov_cmp_a_path = "com_los_prov_cmp_a.csv"

# Read data into DataFrames
matched_los_reg_22 = pd.read_csv(matched_los_reg_22_path)
matched_tpia_reg_22 = pd.read_csv(matched_tpia_reg_22_path)
supp_los_supp_reg_22 = pd.read_csv(supp_los_supp_reg_22_path)
supp_tpia_supp_reg_22 = pd.read_csv(supp_tpia_supp_reg_22_path)
ed_facility_org_22 = pd.read_csv(ed_facility_org_22_path)
com_tpia_org_cmp_a = pd.read_csv(com_tpia_org_cmp_a_path)
com_los_org_cmp_a = pd.read_csv(com_los_org_cmp_a_path)
com_tpia_reg_cmp_a = pd.read_csv(com_tpia_reg_cmp_a_path)
com_los_reg_cmp_a = pd.read_csv(com_los_reg_cmp_a_path)
com_tpia_prov_cmp_a = pd.read_csv(com_tpia_prov_cmp_a_path)
com_los_prov_cmp_a = pd.read_csv(com_los_prov_cmp_a_path)

# Define the additional fiscal year
add_yr = 22

# Rename columns in DataFrames
matched_los_reg_22.rename(columns={"new_region_id": "region_id"}, inplace=True)
matched_tpia_reg_22.rename(columns={"new_region_id": "region_id"}, inplace=True)
supp_los_supp_reg_22.rename(columns={"new_region_id": "region_id"}, inplace=True)
supp_tpia_supp_reg_22.rename(columns={"new_region_id": "region_id"}, inplace=True)

# Create a filtered DataFrame based on conditions
ed_nacrs_flg_1_22 = ed_facility_org_22[(ed_facility_org_22["nacrs_ed_flg"] == 1) & (ed_facility_org_22["corp_cnt"] == 1)]

# Define a function to compile data
def compile(yr, ind, report, id):
    if report == "org":
        # Organizational level
        com_trd_a = pd.merge(matched_tpia_reg_22, com_tpia_org_cmp_a, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, com_tpia_reg_cmp_a, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, matched_los_reg_22, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, com_los_org_cmp_a, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, com_los_reg_cmp_a, on=id, how="left")

        com_trd_a.to_csv(f"{ind}_{report}_com_trd_{yr}_a.csv", index=False)

        com_trd_a.sort_values(by=id, inplace=True)
        ed_nacrs_flg_1_22.sort_values(by=id, inplace=True)

        com_trd_yr = pd.merge(com_trd_a, ed_nacrs_flg_1_22, on=id, how="right", indicator=True)
        com_trd_yr = com_trd_yr[com_trd_yr["_merge"] == "right_only"]
        com_trd_yr.drop(columns=["_merge"], inplace=True)

        com_trd_yr.rename(columns={"nacrs_ed_flg": "nacrs_ed_flg_removed"}, inplace=True)
        com_trd_yr.to_csv(f"{ind}_{report}_com_trd_{yr}.csv", index=False)
    
    elif report in ("reg", "prov"):
        # Region and Province level
        com_trd_a = pd.merge(matched_tpia_reg_22, com_tpia_reg_cmp_a, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, matched_los_reg_22, on=id, how="left")
        com_trd_a = pd.merge(com_trd_a, com_los_reg_cmp_a, on=id, how="left")

        com_trd_a.to_csv(f"{ind}_{report}_com_trd_{yr}_a.csv", index=False)
    
# Compile data for different scenarios
compile(add_yr, "tpia", "org", "corp_id")
compile(add_yr, "tpia", "reg", "region_id")
compile(add_yr, "tpia", "prov", "province_id")
compile(add_yr, "los", "org", "corp_id")
compile(add_yr, "los", "reg", "region_id")
compile(add_yr, "los", "prov", "province_id")

# Update previous fiscal year data
def remove_prev_year_data(yr, ind_id):
    hsp_ind_organization_fact_path = f"hsp_ind_organization_fact{ind_id}.csv"
    hsp_ind_organization_fact = pd.read_csv(hsp_ind_organization_fact_path)
    
    hsp_ind_organization_fact = hsp_ind_organization_fact[~(hsp_ind_organization_fact["FISCAL_YEAR_WH_ID"] == yr - 5)]
    hsp_ind_organization_fact.to_csv(hsp_ind_organization_fact_path, index=False)

# Remove previous fiscal year data for organizations 33 and 34
remove_prev_year_data(add_yr, 33)
remove_prev_year_data(add_yr, 34)

# Filter and save test data for organizations 33 and 34
test1 = hsp_ind_organization_fact33[hsp_ind_organization_fact33["organization_id"].isin([1019, 10038, 5085, 5049])]
test2 = hsp_ind_organization_fact34[hsp_ind_organization_fact34["organization_id"].isin([1048, 5160, 81118, 81170])]

test1.to_csv("test1.csv", index=False)
test2.to_csv("test2.csv", index=False)

# Change organization_id values based on README document
hsp_ind_organization_fact33.loc[hsp_ind_organization_fact33["organization_id"] == 5085, "organization_id"] = 81180
hsp_ind_organization_fact33.loc[hsp_ind_organization_fact33["organization_id"] == 5049, "organization_id"] = 81263

hsp_ind_organization_fact34.loc[hsp_ind_organization_fact34["organization_id"] == 81118, "organization_id"] = 81180

# Filter and save test data for organization 34 after changing organization_id values
test34 = hsp_ind_organization_fact34[hsp_ind_organization_fact34["organization_id"] == 81180]
test34.to_csv("test34.csv", index=False)

# Define a function to append new fiscal year data
def append_new_year_data(yr, ind, ind_id, report, id):
    if report == "prov":
        # Province level
        compare_ind_code = "999"
        improvement_ind_code = "999"
    elif report in ("reg", "org"):
        # Region and Organization level
        compare_ind_code = ""
        improvement_ind_code = ""

    if report == "prov":
        data_period_code = f"0{yr}"
    else:
        data_period_code = f"0{yr}"

    data_period_type_code = "FY"

    if report == "peer":
        peer_group_ids = ['H1', 'H2', 'H3', 'T']

        new_year_data = matched_tpia_reg_22[matched_tpia_reg_22["peer_group_id"].isin(peer_group_ids)]
    elif report == "nt":
        new_year_data = matched_tpia_reg_22
    else:
        new_year_data = pd.read_csv(f"{ind}_{report}_com_trd_{yr}.csv")

    new_year_data["ORGANIZATION_ID"] = id
    new_year_data["INDICATOR_CODE"] = f"0{ind_id}"
    new_year_data["FISCAL_YEAR_WH_ID"] = yr
    new_year_data["SEX_WH_ID"] = 3
    new_year_data["INDICATOR_SUPPRESSION_CODE"] = "007"
    new_year_data["TOP_PERFORMER_IND_CODE"] = "999"
    new_year_data["IMPROVEMENT_IND_CODE"] = improvement_ind_code
    new_year_data["COMPARE_IND_CODE"] = compare_ind_code
    new_year_data["INDICATOR_VALUE"] = new_year_data["percentile_90"].quantile(0.1).round(1)
    new_year_data["DATA_PERIOD_CODE"] = data_period_code
    new_year_data["DATA_PERIOD_TYPE_CODE"] = data_period_type_code

    new_year_data.to_csv(f"hsp_ind_organization_fact{ind_id}.csv", mode="a", header=False, index=False)

# Append new fiscal year data for different scenarios
append_new_year_data(add_yr, "tpia", 33, "org", "corp_id")
append_new_year_data(add_yr, "tpia", 33, "reg", "region_id")
append_new_year_data(add_yr, "tpia", 33, "prov", "province_id")
append_new_year_data(add_yr, "los", 33, "org", "corp_id")
append_new_year_data(add_yr, "los", 33, "reg", "region_id")
append_new_year_data(add_yr, "los", 33, "prov", "province_id")
append_new_year_data(add_yr, "tpia", 34, "org", "corp_id")
append_new_year_data(add_yr, "tpia", 34, "reg", "region_id")
append_new_year_data(add_yr, "tpia", 34, "prov", "province_id")
append_new_year_data(add_yr, "los", 34, "org", "corp_id")
append_new_year_data(add_yr, "los", 34, "reg", "region_id")
append_new_year_data(add_yr, "los", 34, "prov", "province_id")

