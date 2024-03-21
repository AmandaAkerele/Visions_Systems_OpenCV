# Convert COMPARE_IND_CODE column to numeric type
EDWT_Indicators["COMPARE_IND_CODE"] = pd.to_numeric(EDWT_Indicators["COMPARE_IND_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["COMPARE_IND_CODE"].notna()) & (EDWT_Indicators["COMPARE_IND_CODE"] != 999)]

# Check the column again to confirm if the rows with '999' are dropped
EDWT_Indicators["COMPARE_IND_CODE"].value_counts()
