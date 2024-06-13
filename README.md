---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_364/2218656056.py in <cell line: 4>()
      2 
      3 # Directly compute the number of years each CORP_ID was in the PERCENTILE_90
----> 4 corp_count = tpia_org_all.withColumn(
      5     "in_percentile_90",
      6     when(col("PERCENTILE_90").isNotNull(), 1).otherwise(0)

AttributeError: 'list' object has no attribute 'withColumn'
