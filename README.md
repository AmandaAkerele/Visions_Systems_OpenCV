from pyspark.sql.functions import when, col

for c in los_org_cmp.columns:
    los_org_cmp = los_org_cmp.withColumn(c, when(col(c) == 0.2, '20th_Percentile').when(col(c) == 0.8, '80th_Percentile').otherwise(col(c)))
