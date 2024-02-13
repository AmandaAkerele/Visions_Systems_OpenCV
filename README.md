from pyspark.sql.functions import lit, col

# Get the list of columns from tmp_ed_facility_org_a in lowercase
all_columns_lower = [c.lower() for c in tmp_ed_facility_org_a.columns]

# Add missing columns to t3 and t4 with case-insensitive comparison
for col_name in tmp_ed_facility_org_a.columns:
    col_name_lower = col_name.lower()
    if col_name_lower not in [c.lower() for c in t3.columns]:
        t3 = t3.withColumn(col_name, lit(None))
    if col_name_lower not in [c.lower() for c in t4.columns]:
        t4 = t4.withColumn(col_name, lit(None))

# Now perform the unionByName operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True).unionByName(t4, allowMissingColumns=True).distinct()


or 
from pyspark.sql import DataFrame

def rename_columns_to_uppercase(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.upper())
    return df

# Applying the function to your DataFrames
tmp_ed_facility_org_a = rename_columns_to_uppercase(tmp_ed_facility_org_a)
t3 = rename_columns_to_uppercase(t3)
t4 = rename_columns_to_uppercase(t4)

# Union the DataFrames
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3).unionByName(t4).distinct()

