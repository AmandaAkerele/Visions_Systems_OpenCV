---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_324/595275628.py in <cell line: 22>()
     20 
     21 # Apply the OLS Pandas UDF to the DataFrame grouped by 'CORP_ID'
---> 22 regression_results = los_org_all_yr_b.groupby('CORP_ID').apply(ols_regression)
     23 
     24 # Show the results

/usr/local/lib/python3.10/dist-packages/pyspark/sql/pandas/group_ops.py in apply(self, udf)
    109         )
    110 
--> 111         return self.applyInPandas(udf.func, schema=udf.returnType)  # type: ignore[attr-defined]
    112 
    113     def applyInPandas(

/usr/local/lib/python3.10/dist-packages/pyspark/sql/pandas/group_ops.py in applyInPandas(self, func, schema)
    228         udf = pandas_udf(func, returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
    229         df = self._df
--> 230         udf_column = udf(*[df[col] for col in df.columns])
    231         jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
    232         return DataFrame(jdf, self.session)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/pandas/group_ops.py in <listcomp>(.0)
    228         udf = pandas_udf(func, returnType=schema, functionType=PandasUDFType.GROUPED_MAP)
    229         df = self._df
--> 230         udf_column = udf(*[df[col] for col in df.columns])
    231         jdf = self._jgd.flatMapGroupsInPandas(udf_column._jc.expr())
    232         return DataFrame(jdf, self.session)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getitem__(self, item)
   3072         """
   3073         if isinstance(item, str):
-> 3074             jc = self._jdf.apply(item)
   3075             return Column(jc)
   3076         elif isinstance(item, Column):

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `CORP_PEER` is ambiguous, could be: [`CORP_PEER`, `CORP_PEER`].


