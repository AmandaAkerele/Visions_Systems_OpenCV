# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].notna()) & (EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] != 999)]

# Check the column again to confirm if the rows with '999' are dropped
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].value_counts()

New column 
add mapping for INDICATOR_SUPPRESSION_CODE
7: NA (The indicator value is not suppressed)
2: Data not available due to data quality issues
6: Data not reported due to incomplete data submission
901: Indicator data manually suppressed at Program Area’s request
