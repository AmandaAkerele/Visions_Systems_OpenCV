# # Create standalone DataFrame
stand_alone = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select('CORP_ID').distinct()

# Create multiple_sites DataFrame
multiple_sites = df_fac.join(stand_alone, 'CORP_ID', 'inner') \
    .groupBy('CORP_ID').agg(count('*').alias('cnt_sites')) \
    .filter(col('cnt_sites') > 1)

# Alias the datasets 

df_fac_alias = df_fac.alias("fac")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Filter ed_facility_org for specific conditions and alias the result
ed_facility_org_filtered = ed_facility_org_alias.filter(
    (col("edfo.TYPE") == 'SL') & (col("edfo.CORP_CNT") == 1)
).select("edfo.CORP_ID")

# Perform the join operation

ed_nacrs_flg_1_22 = df_fac_alias.join(
    ed_facility_org_filtered, 
    col("fac.CORP_ID") == col("edfo.CORP_ID")
).filter(col("fac.NACRS_ED_FLG") == 1)
    


# Alias the DataFrames
tpia_org_22_alias = tpia_org_22.alias("tpia22")
ed_nacrs_flg_1_22_alias = ed_nacrs_flg_1_22.alias("ednacrs22")
tpia_supp_org_alias = tpia_supp_org.alias("tpiasupp")
ed_facility_org_alias = ed_facility_org.alias("edfo")

# Prepare the set of conditions for filtering
tpia_corp_conditions = (
    ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in ed_nacrs_flg_1_22_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in tpia_supp_org_alias.select("CORP_ID").collect()]) &
    ~col("tpia22.CORP_ID").isin(
        [row.CORP_ID for row in ed_facility_org_alias.filter(
            (col("edfo.SUBMISSION_FISCAL_YEAR") == "2022") &
            ((col("edfo.TYPE") == 'PS') & (col("edfo.CORP_CNT") == 1) |
             (col("edfo.TYPE") == 'DQ') & (col("edfo.CORP_CNT") == 1) & (col("edfo.IND") == 'TPIA'))
        ).select("edfo.CORP_ID").collect()]
    )
)

# Apply the filter conditions
# tpia_org_ta = tpia_org_22_alias.filter(tpia_corp_conditions)

---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_287/2782838860.py in <cell line: 9>()
      7 # Prepare the set of conditions for filtering
      8 tpia_corp_conditions = (
----> 9     ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in ed_nacrs_flg_1_22_alias.select("CORP_ID").collect()]) &
     10     ~col("tpia22.CORP_ID").isin([row.CORP_ID for row in tpia_supp_org_alias.select("CORP_ID").collect()]) &
     11     ~col("tpia22.CORP_ID").isin(

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

AnalysisException: [AMBIGUOUS_REFERENCE] Reference `CORP_ID` is ambiguous, could be: [`ednacrs22`.`CORP_ID`, `ednacrs22`.`CORP_ID`].



