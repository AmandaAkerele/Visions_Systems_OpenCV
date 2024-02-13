from pyspark.sql.functions import col

# List to keep track of seen columns
seen_columns = set()
unique_columns = []

for column in tmp_ed_facility_org_a.columns:
    if column not in seen_columns:
        unique_columns.append(column)
        seen_columns.add(column)

# Selecting only the unique columns from tmp_ed_facility_org_a
tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(*unique_columns)



and 
# Perform the union operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True) \
                                           .unionByName(t4, allowMissingColumns=True) \
                                           .distinct()

combin e
from pyspark.sql.functions import col

# Step 1: Remove duplicate 'CORP_ID' column from tmp_ed_facility_org_a
seen_columns = set()
unique_columns = []

for column in tmp_ed_facility_org_a.columns:
    if column not in seen_columns:
        unique_columns.append(column)
        seen_columns.add(column)

tmp_ed_facility_org_a = tmp_ed_facility_org_a.select(*unique_columns)

# Step 2: Perform the union operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True) \
                                           .unionByName(t4, allowMissingColumns=True) \
                                           .distinct()









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





