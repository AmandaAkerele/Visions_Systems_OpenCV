AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_289/3926978093.py in <cell line: 2>()
      1 # Union the DataFrames
----> 2 tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3).unionByName(t4).distinct()

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in unionByName(self, other, allowMissingColumns)
   4035         +----+----+----+----+----+----+
   4036         """
-> 4037         return DataFrame(self._jdf.unionByName(other._jdf, allowMissingColumns), self.sparkSession)
   4038 
   4039     def intersect(self, other: "DataFrame") -> "DataFrame":

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

AnalysisException: [COLUMN_ALREADY_EXISTS] The column `corp_id` already exists. Consider to choose another name or rename the existing column.
