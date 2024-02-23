---------------------------------------------------------------------------
PySparkTypeError                          Traceback (most recent call last)
/tmp/ipykernel_2055/2107409859.py in <cell line: 18>()
     16 # Repeat for other DataFrames
     17 los_reg_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'NEW_REGION_ID', 'REGION_E_DESC'])
---> 18 los_reg_22 = rename_columns_upper(los_reg_22)
     19 
     20 los_prov_22 = calculate_percentile_spark(los_nt_record_ucc_22, 'LOS_HOURS', [0.9], ['SUBMISSION_FISCAL_YEAR', 'FACILITY_PROVINCE', 'PROVINCE_ID', 'PROVINCE_NAME'])

/tmp/ipykernel_2055/1638686559.py in rename_columns_upper(df)
      6     for c in df.columns:
      7         if isinstance(df.schema[c].dataType, StringType):
----> 8             df = df.withColumnRenamed(c, upper(c))
      9     return df
     10 

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in withColumnRenamed(self, existing, new)
   5202         +----+-----+
   5203         """
-> 5204         return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sparkSession)
   5205 
   5206     def withColumnsRenamed(self, colsMap: Dict[str, str]) -> "DataFrame":

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in __call__(self, *args)
   1312 
   1313     def __call__(self, *args):
-> 1314         args_command, temp_args = self._build_args(*args)
   1315 
   1316         command = proto.CALL_COMMAND_NAME +\

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in _build_args(self, *args)
   1275     def _build_args(self, *args):
   1276         if self.converters is not None and len(self.converters) > 0:
-> 1277             (new_args, temp_args) = self._get_args(args)
   1278         else:
   1279             new_args = args

/usr/local/lib/python3.10/dist-packages/py4j/java_gateway.py in _get_args(self, args)
   1262                 for converter in self.gateway_client.converters:
   1263                     if converter.can_convert(arg):
-> 1264                         temp_arg = converter.convert(arg, self.gateway_client)
   1265                         temp_args.append(temp_arg)
   1266                         new_args.append(temp_arg)

/usr/local/lib/python3.10/dist-packages/py4j/java_collections.py in convert(self, object, gateway_client)
    508         ArrayList = JavaClass("java.util.ArrayList", gateway_client)
    509         java_list = ArrayList()
--> 510         for element in object:
    511             java_list.add(element)
    512         return java_list

/usr/local/lib/python3.10/dist-packages/pyspark/sql/column.py in __iter__(self)
    716 
    717     def __iter__(self) -> None:
--> 718         raise PySparkTypeError(
    719             error_class="NOT_ITERABLE", message_parameters={"objectName": "Column"}
    720         )

PySparkTypeError: [NOT_ITERABLE] Column is not iterable.
