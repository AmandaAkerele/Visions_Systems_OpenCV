# Remove suppressed corp and non-reported corps for TPIA
tpia_corp_conditions = (~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22.select('CORP_ID')) &
                        ~tpia_org_22['CORP_ID'].isin(tpia_supp_org.select('CORP_ID')) &
                        ~tpia_org_22['CORP_ID'].isin(
                            ed_facility_org.filter(
                                (ed_facility_org['SUBMISSION_FISCAL_YEAR'] == "2022") &
                                ((ed_facility_org['TYPE'] == 'PS') & (ed_facility_org['CORP_CNT'] == 1) |
                                 (ed_facility_org['TYPE'] == 'DQ') & (ed_facility_org['CORP_CNT'] == 1) & (ed_facility_org['IND'] == 'TPIA'))
                            ).select('CORP_ID')
                        ))
tpia_org_ta = tpia_org_22.filter(tpia_corp_conditions)

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_287/3648305838.py in <cell line: 2>()
      1 # Remove suppressed corp and non-reported corps for TPIA
----> 2 tpia_corp_conditions = (~tpia_org_22['CORP_ID'].isin(ed_nacrs_flg_1_22.select('CORP_ID')) &
      3                         ~tpia_org_22['CORP_ID'].isin(tpia_supp_org.select('CORP_ID')) &
      4                         ~tpia_org_22['CORP_ID'].isin(
      5                             ed_facility_org.filter(

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
   3221         +-----+---+
   3222         """
-> 3223         jdf = self._jdf.select(self._jcols(*cols))
   3224         return DataFrame(jdf, self.sparkSession)
   3225 

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `CORP_ID` is ambiguous, could be: [`edfo`.`CORP_ID`, `fac`.`CORP_ID`].


orrr


from pyspark.sql.functions import col, count

# Create standalone DataFrame
stand_alone = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('CORP_ID')

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner')
multiple_sites = multiple_sites.groupBy('CORP_ID').agg(count('*').alias('cnt_sites'))
multiple_sites = multiple_sites.filter(col('cnt_sites') > 1)

