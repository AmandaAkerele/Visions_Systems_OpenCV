# List of all column names in tmp_ed_facility_org_a
all_columns = tmp_ed_facility_org_a.columns

# Add missing columns to t3 and t4
for col in all_columns:
    if col not in t3.columns:
        t3 = t3.withColumn(col, lit(None))
    if col not in t4.columns:
        t4 = t4.withColumn(col, lit(None))

tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3).unionByName(t4).distinct()


correct this error message that says  [COLUMN_ALREADY_EXISTS] The column `corp_id` already exists. Consider to choose another name or rename the existing column.
