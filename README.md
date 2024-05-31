# Continue with the code for ED_CORP_20&yr for calculating suppression and 90th percentile
ED_CORP_22_df = ed_nodup_noucc_nosb_22.groupby('CORP_ID').agg({'AM_CARE_KEY':'count'}).reset_index().rename(columns={'AM_CARE_KEY': 'ED_CNT'})


help resolve code 

---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_290/3047032381.py in <cell line: 2>()
      1 # Continue with the code for ED_CORP_20&yr for calculating suppression and 90th percentile
----> 2 ED_CORP_22_df = ed_nodup_noucc_nosb_22.groupby('CORP_ID').agg({'AM_CARE_KEY':'count'}).reset_index().rename(columns={'AM_CARE_KEY': 'ED_CNT'})

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3121         """
   3122         if name not in self.columns:
-> 3123             raise AttributeError(
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )

AttributeError: 'DataFrame' object has no attribute 'reset_index'
