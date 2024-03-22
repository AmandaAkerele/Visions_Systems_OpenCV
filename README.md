# Convert INDICATOR_SUPPRESSION_CODE column to numeric type
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] = pd.to_numeric(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"], errors='coerce')

# Drop rows with NaN and '999' in the COMPARE_IND_CODE column
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].notna()) & (EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"] != 999)]

# Check the column again to confirm if the rows with '999' are dropped
EDWT_Indicators["INDICATOR_SUPPRESSION_CODE"].value_counts()


7: <leave blank>
2: S03
6: S10
901: S08
