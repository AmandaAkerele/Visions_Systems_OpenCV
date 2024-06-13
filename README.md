recreate this code 


# Define the window specification to partition by ORG_ID and order by year
windowSpec = Window.partitionBy("CORP_ID").orderBy("SUBMISSION_FISCAL_YEAR")
# Add a column to flag the rows that are in the PERCENTILE_90
corp_all = corp_all.withColumn("in_percentile_90", F.when(F.col("PERCENTILE_90"). isNotNull(), 1).otherwise(0))
#corp_all.show(5)
# Aggregate to count the number of years in PERCENTILE_90 for each ORG_ID
corp_count = corp_all.groupBy("CORP_ID").agg(F.sum("in_percentile_90").alias("years_in_percentile_90"))
#df_count.show(5)
# Filter ORG_IDs with at least 3 years in PERCENTILE_90
corp_filtered = corp_count.filter(F.col("years_in_percentile_90") >= 3)
# Join back with the original DataFrame to get the full records for these ORG_IDs
corp_comp = corp_all.join(corp_filtered, on="CORP_ID", how="inner").filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
corp_trend = corp_all.join(corp_filtered, on="CORP_ID", how="inner") \
    .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(str(int(closed_year)-1), str(int(closed_year)-2), str(int(closed_year)-3))) 
# Drop the temporary column   str(closed_year)
#los_org_3x3 = los_org_3x3.drop("in_percentile_90", "years_in_percentile_90")
# Show the result
#corp_comp.show()
