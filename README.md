# For Peer Level 
from pyspark.sql import functions as F

# Create 'tpia_rec' column with conditions
tpia_peer_cnt_a_df = ed_record_22_Peer.withColumn(
    'tpia_rec', 
    F.when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT').isNull() | (F.trim(F.col('TIME_PHYSICAN_INIT_ASSESSMENT')) == ''), 'B')
     .when(F.col('TIME_PHYSICAN_INIT_ASSESSMENT') == '9999', 'N')
     .otherwise('Y')
)

# Filter out rows with 'tpia_rec' equal to 'B'
tpia_peer_cnt_df = tpia_peer_cnt_a_df.filter(F.col('tpia_rec') != 'B')

# Aggregating data for tpia_peer_rec_df
tpia_peer_rec_df = tpia_peer_cnt_df.groupBy('SUBMISSION_FISCAL_YEAR', 'SITE_PEER').agg(
    F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
    F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
).withColumn('tpia_rec_pct', F.col('tpia_calc_cnt') / F.count(F.lit(1)))

# Creating tpia_supp_peer_df Dataframe
tpia_supp_peer_df = tpia_peer_rec_df.filter(
    (F.col('tpia_calc_cnt') < 50) | ((F.col('tpia_calc_cnt') > 50) & (F.col('tpia_rec_pct') < 0.75))
)

correct this error below for peer level 


---------------------------------------------------------------------------
AnalysisException                         Traceback (most recent call last)
/tmp/ipykernel_1253/3847514628.py in <cell line: 2>()
      3     F.sum(F.when(F.col('tpia_rec') == 'Y', 1).otherwise(0)).alias('tpia_calc_cnt'),
      4     F.sum(F.when(F.col('tpia_rec').isin(['Y', 'N']), 1).otherwise(0)).alias('tpia_elig_cnt')
----> 5 ).withColumn('tpia_rec_pct', F.col('tpia_calc_cnt') / F.count(F.lit(1)))
      6 
      7 # Creating tpia_supp_peer_df Dataframe

/usr/local/lib/python3.10/dist-packages/pyspark/sql/dataframe.py in withColumn(self, colName, col)
   5168                 message_parameters={"arg_name": "col", "arg_type": type(col).__name__},
   5169             )
-> 5170         return DataFrame(self._jdf.withColumn(colName, col._jc), self.sparkSession)
   5171 
   5172     def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":

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

AnalysisException: [MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY or turn it into the window functions using OVER clauses.;
