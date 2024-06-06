from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def calculate_percentile(df, column, percentiles, bycols=None, confidence_interval=False):
    # Ensure percentiles are expressed as floating point
    percentiles = [float(p) for p in percentiles]

    if bycols:
        # Calculate percentiles grouped by specified columns
        # Using the correct formatting for array creation in Spark SQL
        results = df.groupBy(bycols).agg(*[
            F.expr(f"percentile_approx({column}, array({','.join(map(str, percentiles))}), 0.01)").alias(f"PERCENTILE_{int(p * 100)}")
            for p in percentiles
        ])
    else:
        # Calculate global percentiles for the entire dataframe with a lower relative error for more accuracy
        global_percentiles = {f"PERCENTILE_{int(p * 100)}": df.approxQuantile(column, [p], 0.01)[0] for p in percentiles}
        results = spark.createDataFrame([global_percentiles])

    # Convert column names to upper case if needed
    return results.select([F.col(c).alias(c.upper()) for c in results.columns])

# Example usage of the function
tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0, 0.5, 0.9, 0.999, 1])
tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0.9], bycols=["SUBMISSION_FISCAL_YEAR", 'adm_prov_plldj_name_e_desc', 'adm_region_id', 'adm_region_pll_name_e_desc'])



                                                                                
---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_13391/1540476669.py in <cell line: 26>()
     24 # Example usage of the function
     25 tpia_nt_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0, 0.5, 0.9, 0.999, 1])
---> 26 tpia_reg_22 = calculate_percentile(TPIA_nt_record_ucc_22, "WAIT_TIME_TO_PIA_HOURS", [0.9], bycols=["SUBMISSION_FISCAL_YEAR", 'adm_prov_plldj_name_e_desc', 'adm_region_id', 'adm_region_pll_name_e_desc'])

/tmp/ipykernel_13391/1540476669.py in calculate_percentile(df, column, percentiles, bycols, confidence_interval)
     10         # Calculate percentiles grouped by specified columns
     11         # Using the correct formatting for array creation in Spark SQL
---> 12         results = df.groupBy(bycols).agg(*[
     13             F.expr(f"percentile_approx({column}, array({','.join(map(str, percentiles))}), 0.01)").alias(f"PERCENTILE_{int(p * 100)}")
     14             for p in percentiles

/usr/local/lib/python3.10/dist-packages/pyspark/sql/group.py in agg(self, *exprs)
    184             assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
    185             exprs = cast(Tuple[Column, ...], exprs)
--> 186             jdf = self._jgd.agg(exprs[0]._jc, _to_seq(self.session._sc, [c._jc for c in exprs[1:]]))
    187         return DataFrame(jdf, self.session)
    188 

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

AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "percentile_approx(WAIT_TIME_TO_PIA_HOURS, array(0.9), 0.01)" due to data type mismatch: Parameter 3 requires the "INTEGRAL" type, however "0.01" has the type "DECIMAL(2,2)".; line 1 pos 0;
