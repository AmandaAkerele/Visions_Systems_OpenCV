AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_292/3789467198.py in <cell line: 2>()
      1 # Convert the Table to a pandas DataFrame
----> 2 nacrs_yr_df2 = nacrs_yr_df.to_pandas()

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3121         """
   3122         if name not in self.columns:
-> 3123             raise AttributeError(
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )

AttributeError: 'DataFrame' object has no attribute 'to_pandas'
