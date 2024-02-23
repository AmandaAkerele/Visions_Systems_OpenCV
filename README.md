AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_2055/1680917666.py in <cell line: 17>()
     15     return df
     16 # Calculation for los_nt_22
---> 17 los_nt_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1])
     18 
     19 # Calculation for los_reg_22

/tmp/ipykernel_2055/1680917666.py in calculate_percentile_spark(df, column, percentiles, bycols)
     11         for percentile in percentiles:
     12             percentile_col_name = f'percentile_{percentile}'
---> 13             df = df.withColumn(percentile_col_name, percentile_approx(col(column), percentile, 10000))
     14 
     15     return df

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in withColumn(self, colName, col)
   5168                 message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
   5169             )
-> 5170         return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)
   5171 
   5172     def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":

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

AnalysisException: [MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY or turn it into the window functions using OVER clauses.;
