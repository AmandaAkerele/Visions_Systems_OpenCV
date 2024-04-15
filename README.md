renaming_expr = [when(col(c) == 0.2, '20th_Percentile').when(col(c) == 0.8, '80th_Percentile').otherwise(col(c)) for c in los_org_cmp.columns]
los_org_cmp_a = los_org_cmp.select([renaming_expr])


solve the error below using another method for the code above 
---------------------------------------------------------------------------
PySparkTypeError                          Traceback (most recent call last)
/tmp/ipykernel_1040/1585020647.py in <cell line: 7>()
      5 
      6 renaming_expr = [when(col(c) == 0.2, '20th_Percentile').when(col(c) == 0.8, '80th_Percentile').otherwise(col(c)) for c in los_org_cmp.columns]
----> 7 los_org_cmp_a = los_org_cmp.select([renaming_expr])

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in select(self, *cols)
   3221         +-----+---+
   3222         """
-> 3223         jdf = self._jdf.select(self._jcols(*cols))
   3224         return DataFrame(jdf, self.sparkSession)
   3225 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in _jcols(self, *cols)
   2758         if len(cols) == 1 and isinstance(cols[0], list):
   2759             cols = cols[0]
-> 2760         return self._jseq(cols, _to_java_column)
   2761 
   2762     def _sort_cols(

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in _jseq(self, cols, converter)
   2745     ) -> JavaObject:
   2746         """Return a JVM Seq of Columns from a list of Column or names"""
-> 2747         return _to_seq(self.sparkSession._sc, cols, converter)
   2748 
   2749     def _jmap(self, jm: Dict) -> JavaObject:

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in _to_seq(sc, cols, converter)
     86     """
     87     if converter:
---> 88         cols = [converter(c) for c in cols]
     89     assert sc._jvm is not None
     90     return sc._jvm.PythonUtils.toSeq(cols)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in <listcomp>(.0)
     86     """
     87     if converter:
---> 88         cols = [converter(c) for c in cols]
     89     assert sc._jvm is not None
     90     return sc._jvm.PythonUtils.toSeq(cols)

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in _to_java_column(col)
     63         jcol = _create_column_from_name(col)
     64     else:
---> 65         raise PySparkTypeError(
     66             error_class="NOT_COLUMN_OR_STR",
     67             message_parameters={"arg_name": "col", "arg_type": type(col).__name__},

PySparkTypeError: [NOT_COLUMN_OR_STR] Argument `col` should be a Column or str, got list.
