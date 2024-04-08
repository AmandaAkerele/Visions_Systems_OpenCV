solve the error below for the code above 

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_1015/3833350077.py in <cell line: 36>()
     34 
     35 # Call the calculate_percentile function
---> 36 los_regg = calculate_percentile(ed_record_admit_with_ucc_22, 'LOS_HOURS', [0.9], confidence_interval=True)
     37 los_regg.show()
     38 

/tmp/ipykernel_1015/3833350077.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     14         # Compute point estimate
     15         percentile_val = ppt[i]
---> 16         df = df.withColumn("rank", F.percent_rank().over(windowSpec))
     17         df = df.withColumn("point_estimate", F.first(metric).over(windowSpec).alias(f'point_estimate_{strg[i]}'))
     18 

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

AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `` cannot be resolved. Did you mean one of the following? [`GENDER`, `AGE_NUM`, `CORP_ID`, `SITE_ID`, `CORP_NAME`].;
'Project [FACILITY_AM_CARE_NUM#3, SUBMISSION_FISCAL_YEAR#4, AM_CARE_KEY#0, FACILITY_PROVINCE#2, AMCARE_GROUP_CODE#65, TRIAGE_DATE#24, TRIAGE_TIME#25, DATE_OF_REGISTRATION#27, REGISTRATION_TIME#28, DISPOSITION_DATE#34, DISPOSITION_TIME#35, VISIT_DISPOSITION#36, WAIT_TIME_TO_PIA_HOURS#56, LOS_HOURS#53, WAIT_TIME_TO_INPATIENT_HOURS#57, TIME_PHYSICAN_INIT_ASSESSMENT#30, ED_VISIT_IND_CODE#68, GENDER#10, AGE_NUM#55, SITE_ID#5307, SITE_NAME#5308, SITE_PEER#5309, CORP_ID#5310, CORP_NAME#5311, CORP_PEER#5312, NEW_REGION_ID#5323, REGION_E_DESC#5324, PROVINCE_ID#5315, PROVINCE_NAME#5316, NACRS_ED_FLG#5320, percent_rank() windowspecdefinition(', LOS_HOURS#53 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#17170]
+- Filter NOT FACILITY_AM_CARE_NUM#3 IN ()
