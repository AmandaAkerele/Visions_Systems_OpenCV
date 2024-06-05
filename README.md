---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_293/3803884728.py in <cell line: 5>()
      3 
      4 # Group by org_id and corp_id and count occurrences
----> 5 tmp_cnt_ed_facility_org = df_fac.groupBy('org_id', 'corp_id').agg(count('*').alias('CORP_CNT'))
      6 
      7 # Merge t4 with tmp_cnt_ed_facility_org on both org_id and corp_id

/usr/local/lib/python3.10/dist-packages/pyspark/sql/group.py in agg(self, *exprs)
    184             assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
    185             exprs = cast(Tuple[Column, ...], exprs)
--> 186             jdf = self._jgd.agg(exprs[0]._jc, _to_seq(self.session._sc, [c._jc for c in exprs[1:]]))
    187         return DataFrame(jdf, self.session)
    188 

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
