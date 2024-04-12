solve the error in the code below with this 
# Union the datasets using unionByName with allowMissingColumns=True
tpia_org_22 = tpia_org_22.unionByName(filtered_rows_tpia, allowMissingColumns=True)




# Concatenate, merge, and rename for Los Org
los_org_all_yr_b = (
    los_org_20.union(los_org_21).union(los_org_22_a)
      .join(los_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Tpia Org
tpia_org_all_yr_b = (
    tpia_org_20.union(tpia_org_21).union(tpia_org_22_a)
      .join(tpia_org_3x3_spark, on='CORP_ID', how='inner')
      .withColumnRenamed("FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for Los Reg
los_reg_all_yr_b = (
    los_reg_20.union(los_reg_21).union(los_reg_22_a)
      .join(los_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)

# Concatenate, merge, and rename for TPIA Reg
tpia_reg_all_yr_b = (
    tpia_reg_20.union(tpia_reg_21).union(tpia_reg_22_a)
      .join(tpia_reg_3x3_spark, on='REGION_ID', how='inner')
      .withColumnRenamed("SUBMISSION_FISCAL_YEAR", "TIME")
)


error is: 

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_286/2658631015.py in <cell line: 3>()
      1 # Concatenate, merge, and rename for Los Org
      2 los_org_all_yr_b = (
----> 3     los_org_20.union(los_org_21).union(los_org_22_a)
      4       .join(los_org_3x3_spark, on='CORP_ID', how='inner')
      5       .withColumnRenamed("FISCAL_YEAR", "TIME")

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in union(self, other)
   3921         +---+-----+
   3922         """
-> 3923         return DataFrame(self._jdf.union(other._jdf), self.sparkSession)
   3924 
   3925     def unionAll(self, other: "DataFrame") -> "DataFrame":

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1320 
   1321         answer = self.gateway_client.send_command(command)
-> 1322         return_value = get_return_value(
   1323             answer, self.gateway_client, self.target_id, self.name)
   1324 

/usr/local/lib/python3.10/dist-packages/pyspark/errors/exceptions/captured.py in deco(*a, **kw)
    183                 # Hide where the exception came from that shows a non-Pythonic
    184                 # JVM exception message.
--> 185                 raise converted from None
    186             else:
    187                 raise

AnalysisException: [NUM_COLUMNS_MISMATCH] UNION can only be performed on inputs with the same number of columns, but the first input has 4 columns and the third input has 5 columns.;
'Union false, false
