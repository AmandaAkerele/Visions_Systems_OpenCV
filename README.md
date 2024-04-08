---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_1015/2078136804.py in <cell line: 66>()
     64 
     65 # Call the calculate_percentile function
---> 66 los_regg = calculate_percentile(spark_df, 'LOS_HOURS', [0.9], confidence_interval=True)
     67 los_regg.show()

/tmp/ipykernel_1015/2078136804.py in calculate_percentile(df, metric, ppt, confidence_interval, bycols)
     45             df['__'] = 1
     46             bycols = '__'
---> 47         y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
     48         out = y.tolist()
     49         index = y.index.tolist()

/usr/local/lib/python3.10/dist-packages/pyspark/pandas/groupby.py in apply(self, func, *args, **kwargs)
   2029                 pser_or_pdf = grouped[name].apply(pandas_apply, *args, **kwargs)
   2030             else:
-> 2031                 pser_or_pdf = grouped.apply(pandas_apply, *args, **kwargs)
   2032             psser_or_psdf = ps.from_pandas(pser_or_pdf.infer_objects())
   2033 

~/.local/lib/python3.10/site-packages/pandas/core/groupby/groupby.py in apply(self, func, *args, **kwargs)
   1351         with option_context("mode.chained_assignment", None):
   1352             try:
-> 1353                 result = self._python_apply_general(f, self._selected_obj)
   1354             except TypeError:
   1355                 # gh-20949

~/.local/lib/python3.10/site-packages/pandas/core/groupby/groupby.py in _python_apply_general(self, f, data, not_indexed_same, is_transform, is_agg)
   1400             data after applying f
   1401         """
-> 1402         values, mutated = self.grouper.apply(f, data, self.axis)
   1403         if not_indexed_same is None:
   1404             not_indexed_same = mutated

~/.local/lib/python3.10/site-packages/pandas/core/groupby/ops.py in apply(self, f, data, axis)
    765             # group might be modified
    766             group_axes = group.axes
--> 767             res = f(group)
    768             if not mutated and not _is_indexed_like(res, group_axes, axis):
    769                 mutated = True

/usr/local/lib/python3.10/dist-packages/pyspark/pandas/groupby.py in pandas_apply(pdf, *a, **k)
   2006 
   2007             def pandas_apply(pdf: pd.DataFrame, *a: Any, **k: Any) -> Any:
-> 2008                 return f(pdf.drop(groupkey_names, axis=1), *a, **k)
   2009 
   2010         should_return_series = False

/tmp/ipykernel_1015/2078136804.py in <lambda>(x)
     45             df['__'] = 1
     46             bycols = '__'
---> 47         y = df.groupby(bycols).apply(lambda x: percentile_ci(x[metric], ppt[i], confidence_interval))
     48         out = y.tolist()
     49         index = y.index.tolist()

/tmp/ipykernel_1015/2078136804.py in percentile_ci(indata, percentile, confidence_interval)
     10     if ct > 0:
     11         kf = (ct - 1) * percentile
---> 12         pt_low_n = (kf.floor() + 1) - 1
     13         pt_low_n = pt_low_n.where(pt_low_n >= 0, 0)
     14         pt_upp_n = (kf.floor() + 2) - 1

AttributeError: 'float' object has no attribute 'floor'
