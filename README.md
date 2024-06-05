---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_9583/1303251840.py in <cell line: 22>()
     20 
     21 # Join df_fac with SL to add NACRS_ED_FLG column
---> 22 df_fac1 = df_fac.join(SL, (df_fac.corp_id == SL.CORP_ID) & (df_fac.FACILITY_AM_CARE_NUM == SL.FACILITY_AM_CARE_NUM), how='left') \
     23                .select(df_fac["*"], SL["NACRS_ED_FLG"])
     24 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3121         """
   3122         if name not in self.columns:
-> 3123             raise AttributeError(
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )

AttributeError: 'DataFrame' object has no attribute 'FACILITY_AM_CARE_NUM'
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

