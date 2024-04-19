there is a table named los_nt_record_ucc_22 in my dataframe. Why is it saying there is no table: 

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_324/2438287252.py in <cell line: 2>()
      1 # Example usage
----> 2 los_nt_22 = calculate_percentile(spark.table("los_nt_record_ucc_22"), 'LOS_HOURS', [0, 0.5, 0.9, 0.999, 1], True)
      3 los_nt_22.show()
      4 
      5 los_reg_22 = calculate_percentile(spark.table("los_nt_record_ucc_22"), 'LOS_HOURS', [0.9], True, ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])

/usr/local/lib/python3.10/dist-packages/pyspark/sql/session.py in table(self, tableName)
   1665         +---+
   1666         """
-> 1667         return DataFrame(self._jsparkSession.table(tableName), self)
   1668 
   1669     @property

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

AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `los_nt_record_ucc_22` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.;
'UnresolvedRelation [los_nt_record_ucc_22], [], false

