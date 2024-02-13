
from pyspark.sql.functions import round

# Round the double columns
df = df.withColumn("DoubleColumn1_rounded", round("DoubleColumn1", 2))
df = df.withColumn("DoubleColumn2_rounded", round("DoubleColumn2", 2))

# Drop duplicates based on the rounded columns
df = df.dropDuplicates(["DoubleColumn1_rounded", "DoubleColumn2_rounded"])

# Drop the temporary rounded columns
df = df.drop("DoubleColumn1_rounded", "DoubleColumn2_rounded")






# Dropping duplicates in each DataFrame
tmp_ed_facility_org_a = tmp_ed_facility_org_a.dropDuplicates()
t3 = t3.dropDuplicates()
t4 = t4.dropDuplicates()

# Perform the union operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True) \
                                           .unionByName(t4, allowMissingColumns=True) \
                                           .distinct()
