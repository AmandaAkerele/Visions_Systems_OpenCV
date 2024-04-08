import pyspark.sql as sparksql

# Assuming ed_record_admit_with_ucc_22_pandas is your Pandas DataFrame
ed_record_admit_with_ucc_22_spark = sparksql.SparkSession.builder.getOrCreate().createDataFrame(ed_record_admit_with_ucc_22_pandas)
