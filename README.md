# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
all_years_data  = all_years_data[(all_years_data ["COMPARE_IND_CODE"].notna()) & (all_years_data ["COMPARE_IND_CODE"] != 999)]

# Drop rows with NaN and '999' in the IMPROVEMENT_IND_CODE column
all_years_data   = all_years_data [(all_years_data  ["IMPROVEMENT_IND_CODE"].notna()) & (all_years_data  ["IMPROVEMENT_IND_CODE"] != 999)]

all_years_data  = all_years_data [(all_years_data ["INDICATOR_SUPPRESSION_CODE"].notna()) & (all_years_data ["INDICATOR_SUPPRESSION_CODE"] != 999)]
