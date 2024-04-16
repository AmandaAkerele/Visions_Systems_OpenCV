from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Convert to PySpark").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
                                   .withColumn('TIME', F.col('TIME').cast('float'))

# Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN after the conversion
los_org_all_yr_b.createOrReplaceTempView("los_org_all_yr_b")
los_org_all_yr_b = spark.sql("""
    SELECT * 
    FROM los_org_all_yr_b 
    WHERE PERCENTILE_90 IS NOT NULL AND TIME IS NOT NULL
""")

# Group by 'CORP_ID' and perform logistic regression
grouped = los_org_all_yr_b.groupBy('CORP_ID')

# Define a UDF to perform logistic regression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf

def perform_logistic_udf(group_data):
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    data = assembler.transform(group_data)
    
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    model = lr.fit(data)
    
    coef = model.coefficients[0]
    
    residuals = model.summary.residuals.collect()
    n = len(residuals)
    mse = sum([r ** 2 for r in residuals]) / n
    se = np.sqrt(mse / n)
    
    t_val = coef / se
    p_val = model.summary.pValues[0]
    l95b = coef - 1.96 * se
    u95b = coef + 1.96 * se
    
    return coef, se, t_val, p_val, l95b, u95b

perform_logistic_sql = udf(perform_logistic_udf, returnType=FloatType())

results = grouped.agg(perform_logistic_sql(F.collect_list(F.struct('TIME', 'PERCENTILE_90')).alias("data"))).select(
    "CORP_ID",
    "data.coef",
    "data.se",
    "data.t_val",
    "data.p_val",
    "data.l95b",
    "data.u95b"
).withColumnRenamed("coef", "PARAMS").withColumnRenamed("se", "STDERR").withColumnRenamed("t_val", "T").withColumnRenamed("p_val", "PVALUE").withColumnRenamed("l95b", "L95B").withColumnRenamed("u95b", "U95B")

# Step 1: Creating los_org_trend_a with a new variable 'linr'
los_org_trend_a = results.withColumn('linr', 
                                     (F.col('PVALUE') < 0.05) & 
                                     (F.col('PVALUE').isNotNull())).withColumn('linr', F.when(F.col('linr'), 1).otherwise(0))

# Step 2: Creating subsets based on _TYPE_
los_org_p_val = los_org_trend_a.filter(F.col('PVALUE').isNotNull()).select('CORP_ID', 'linr')
los_org_parms = los_org_trend_a.filter(F.col('PARAMS').isNotNull()).select('CORP_ID', 'PARAMS')

# Steps 3 and 4: Sorting and merging the datasets
los_org_p_val_parms = los_org_p_val.join(los_org_parms, 'CORP_ID', 'inner').orderBy('CORP_ID')

# Step 5: Assigning new variables based on conditions
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
                                         .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

mask = (F.col('linr') == 1)
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', 
                                                     F.when(mask & (F.col('PARAMS') > 0), '003')
                                                     .otherwise(F.col('IMPROVEMENT_IND_CODE'))) \
                                         .withColumn('IMPROVEMENT_IND_E_DESC', 
                                                     F.when(mask & (F.col('PARAMS') > 0), 'Weakening')
                                                     .when(mask & (F.col('PARAMS') < 0), 'Improving')
                                                     .otherwise(F.col('IMPROVEMENT_IND_E_DESC')))

# Step 6: Assuming ed_nacrs_flg_1_SL is another DataFrame you have
# Replace 'ed_nacrs_flg_1_SL' with the actual DataFrame
ed_nacrs_flg_1_SL_corp_ids = ed_nacrs_flg_1_SL.select('CORP_ID')
los_org_trend_b = los_org_p_val_parms.join(ed_nacrs_flg_1_SL_corp_ids, 'CORP_ID', 'left_anti')
