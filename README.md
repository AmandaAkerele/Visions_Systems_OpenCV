from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("Logistic Regression Analysis").getOrCreate()

# Assuming 'los_org_all_yr_b' is already defined as a PySpark DataFrame
# Ensure all necessary columns are cast to float where they are expected to be numeric
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
                                   .withColumn('TIME', F.col('TIME').cast('float'))

# Drop any rows where 'PERCENTILE_90' or 'TIME' might be NaN
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Function to perform logistic regression on grouped data
def perform_logistic(group_data):
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    data = assembler.transform(group_data)
    
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    model = lr.fit(data)

    # Extracting coefficients and computing statistics
    coef = model.coefficients[0]
    residuals = model.summary.residuals.rdd.map(lambda x: x[0]**2).collect()
    n = len(residuals)
    mse = sum(residuals) / n
    se = np.sqrt(mse / n)

    t_val = coef / se
    p_val = model.summary.pValues[0]
    l95b = coef - 1.96 * se
    u95b = coef + 1.96 * se

    return {
        'CORP_ID': group_data.select('CORP_ID').first()[0],
        'PARAMS': coef,
        'STDERR': se,
        'T': t_val,
        'PVALUE': p_val,
        'L95B': l95b,
        'U95B': u95b
    }

# Applying the logistic regression function to each group and collecting the results
all_results = [perform_logistic(group) for group in los_org_all_yr_b.groupby('CORP_ID').agg(F.collect_list('TIME').alias('TIME'))]

# Convert results into a DataFrame
los_org_trend = spark.createDataFrame(all_results)

# Additional processing steps to create the final data sets
los_org_trend = los_org_trend.withColumn('linr', (F.col('PVALUE') < 0.05).cast('int'))

# Creating subsets and sorting/merging data as per the original requirement
los_org_p_val = los_org_trend.filter(F.col('_TYPE_') == 'PVALUE').select('CORP_ID', 'linr')
los_org_parms = los_org_trend.filter(F.col('_TYPE_') == 'PARAMS').select('CORP_ID', 'VALUE')
los_org_p_val_parms = los_org_p_val.join(los_org_parms, 'CORP_ID', 'inner').orderBy('CORP_ID')

# Assigning new variables based on conditions
mask = F.col('linr') == 1
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', F.when(mask & (F.col('VALUE') > 0), '003').otherwise('002'))
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_E_DESC', F.when(mask & (F.col('VALUE') > 0), 'Weakening').when(mask & (F.col('VALUE') < 0), 'Improving').otherwise('No Change'))

# Filtering with another DataFrame assuming it's called 'ed_nacrs_flg_1_SL'
ed_nacrs_flg_1_SL_corp_ids = ed_nacrs_flg_1_SL.select('CORP_ID')
los_org_trend_b = los_org_p_val_parms.join(ed_nacrs_flg_1_SL_corp_ids, 'CORP_ID', 'left_anti')
