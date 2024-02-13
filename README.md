from pyspark.sql import DataFrame

def rename_columns_to_uppercase(df: DataFrame) -> DataFrame:
    # Dictionary to track renamed columns to avoid duplicates
    renamed_columns = {}
    for col_name in df.columns:
        upper_col_name = col_name.upper()
        # Check if the uppercase version of the column already exists
        if upper_col_name in renamed_columns:
            # If it exists, generate a new unique name (you can customize this part as needed)
            unique_name = upper_col_name + "_1"
            df = df.withColumnRenamed(col_name, unique_name)
            renamed_columns[unique_name] = None
        else:
            df = df.withColumnRenamed(col_name, upper_col_name)
            renamed_columns[upper_col_name] = None
    return df

# Applying the function to your DataFrames
tmp_ed_facility_org_a = rename_columns_to_uppercase(tmp_ed_facility_org_a)
t3 = rename_columns_to_uppercase(t3)
t4 = rename_columns_to_uppercase(t4)

# Union the DataFrames
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3).unionByName(t4).distinct()


or 

common_columns = set(tmp_ed_facility_org_a.columns).intersection(t3.columns).intersection(t4.columns)

tmp_ed_facility_org_a_common = tmp_ed_facility_org_a.select([F.col(c).alias(c.upper()) for c in common_columns])
t3_common = t3.select([F.col(c).alias(c.upper()) for c in common_columns])
t4_common = t4.select([F.col(c).alias(c.upper()) for c in common_columns])

tmp_ed_facility_org = tmp_ed_facility_org_a_common.unionByName(t3_common).unionByName(t4_common).distinct()





