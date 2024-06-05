solve ther issue below correctly 

# Prepare data for stand alones
alone = spark.createDataFrame(
    [
        (99012, 29061, 1), (80335, 48006, 1), (80335, 48008, 1), (80337, 48015, 1), 
        (80337, 48022, 1), (80337, 48023, 1), (80337, 48024, 1), (80345, 48029, 1), 
        (80338, 48032, 1), (80339, 48037, 1), (80340, 48039, 1), (80344, 48044, 1), 
        (80341, 48053, 1), (80345, 48063, 1), (80347, 48076, 1), (80348, 48083, 1), 
        (80348, 48085, 1), (80348, 48086, 1), (80338, 48116, 1), (80347, 48117, 1), 
        (80337, 48120, 1), (80338, 48121, 1), (5160, 54242, 1), (7043, 71117, 1), 
        (7070, 71163, 1), (973, 88050, 1), (1006, 88080, 1), (986, 88132, 1), 
        (20390, 88142, 1), (99718, 88149, 1), (99724, 88155, 1), (20282, 88349, 1), 
        (20400, 88350, 1), (99725, 88391, 1), (99726, 88394, 1), (80226, 88578, 1), 
        (80517, 88595, 1), (99768, 88922, 1)
    ],
    ['CORP_ID', 'FACILITY_AM_CARE_NUM', 'NACRS_ED_FLG']
)

# Join df_org_dim with ed_nodup_nosb_22 based on org_id
df_fac = df_org_dim.join(ed_nodup_nosb_22, 'org_id')

# Join df_fac with alone to add NACRS_ED_FLG column
df_fac = df_fac.join(alone, (df_fac.corp_id == alone.CORP_ID) & (df_fac.FACILITY_AM_CARE_NUM == alone.FACILITY_AM_CARE_NUM), how='left') \
               .select(df_fac["*"], alone["NACRS_ED_FLG"])

# Fill null NACRS_ED_FLG values with 0
df_fac = df_fac.fillna({'NACRS_ED_FLG': 0})

# Show df_fac with NACRS_ED_FLG
df_fac.show()

# Columns to keep (assuming you want to keep all columns from df_fac)
columns_to_keep = df_fac.columns

# Create DataFrame t3 based on condition
t3 = df_fac.filter(df_fac['NACRS_ED_FLG'] == 1).select(columns_to_keep)
t3 = t3.withColumn('TYPE', lit('SL')).withColumn('IND', lit(''))

# Show t3 DataFrame
t3.show()


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
