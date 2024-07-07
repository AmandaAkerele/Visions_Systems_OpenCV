---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_12720/1688301693.py in <cell line: 1>()
----> 1 tpia_supp_org_ucc_22.filter(tpia_supp_org_ucc_22.year == 2022).show()

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in __getattr__(self, name)
   3121         """
   3122         if name not in self.columns:
-> 3123             raise AttributeError(
   3124                 "'%s' object has no attribute '%s'" % (self.__class__.__name__, name)
   3125             )

AttributeError: 'DataFrame' object has no attribute 'year'
