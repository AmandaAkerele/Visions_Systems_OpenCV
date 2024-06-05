---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_293/2428858143.py in <cell line: 22>()
     20 
     21 # Join df_fac with alone to add NACRS_ED_FLG column
---> 22 df_fac = df_fac.join(alone, (df_fac.corp_id == alone.CORP_ID) & (df_fac.FACILITY_AM_CARE_NUM == alone.FACILITY_AM_CARE_NUM), how='left') \
     23                .select(df_fac["*"], alone["NACRS_ED_FLG"])
     24 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )
-> 3126         jc = self._jdf.apply(name)
   3127         return Column(jc)
   3128 

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `corp_id` is ambiguous, could be: [`corp_id`, `corp_id`].

