from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler

# Initialize Spark session
spark = SparkSession.builder.appName("Convert to PySpark").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
                                   .withColumn('TIME', F.col('TIME').cast('float'))

# Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN after the conversion
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Create a GroupBy object for the 'CORP_ID' variable
grouped = los_org_all_yr_b.groupBy('CORP_ID')

# Define a function to perform logistic regression and return results
def perform_logistic(group_data):
    # Create a vector assembler
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    data = assembler.transform(group_data)

    # Create and fit the model
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    model = lr.fit(data)

    # Extract coefficients
    coef = model.coefficients[0]
    
    # Compute standard error manually
    residuals = model.summary.residuals.collect()
    n = len(residuals)
    mse = sum([r ** 2 for r in residuals]) / n
    se = np.sqrt(mse / n)
    
    t_val = coef / se
    p_val = model.summary.pValues[0]
    l95b = coef - 1.96 * se  # Lower bound for 'time'
    u95b = coef + 1.96 * se  # Upper bound for 'time'

    results_dict = {
        'CORP_ID': group_data.select('CORP_ID').first()[0],
        'PARAMS': coef,
        'STDERR': se,
        'T': t_val,
        'PVALUE': p_val,
        'L95B': l95b,
        'U95B': u95b
    }

    return results_dict

# Perform logistic regression for each group and collect results
all_results = []
for group_name in los_org_all_yr_b.select('TIME').distinct().rdd.flatMap(lambda x: x).collect():
    group_data = los_org_all_yr_b.filter(F.col('TIME') == group_name)
    all_results.append(perform_logistic(group_data))

# Convert results to DataFrame
los_org_trend = spark.createDataFrame(all_results)

# Step 1: Creating los_org_trend_a with a new variable 'linr'
los_org_trend_a = los_org_trend.withColumn('linr', 
                                           (F.col('_TYPE_') == 'PVALUE') & 
                                           (F.col('VALUE') < 0.05) & 
                                           (F.col('VALUE').isNotNull())).withColumn('linr', F.when(F.col('linr'), 1).otherwise(0))

# Step 2: Creating subsets based on _TYPE_
los_org_p_val = los_org_trend_a.filter(F.col('_TYPE_') == 'PVALUE').select('CORP_ID', 'linr')
los_org_parms = los_org_trend_a.filter(F.col('_TYPE_') == 'PARAMS').select('CORP_ID', 'VALUE')

# Steps 3 and 4: Sorting and merging the datasets
los_org_p_val_parms = los_org_p_val.join(los_org_parms, 'CORP_ID', 'inner').orderBy('CORP_ID')

# Step 5: Assigning new variables based on conditions
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', F.lit('002')) \
                                         .withColumn('IMPROVEMENT_IND_E_DESC', F.lit('No Change'))

mask = (F.col('linr') == 1)
los_org_p_val_parms = los_org_p_val_parms.withColumn('IMPROVEMENT_IND_CODE', 
                                                     F.when(mask & (F.col('VALUE') > 0), '003')
                                                     .otherwise(F.col('IMPROVEMENT_IND_CODE'))) \
                                         .withColumn('IMPROVEMENT_IND_E_DESC', 
                                                     F.when(mask & (F.col('VALUE') > 0), 'Weakening')
                                                     .when(mask & (F.col('VALUE') < 0), 'Improving')
                                                     .otherwise(F.col('IMPROVEMENT_IND_E_DESC')))

# Step 6: Assuming ed_nacrs_flg_1_SL is another DataFrame you have
# Replace 'ed_nacrs_flg_1_SL' with the actual DataFrame
ed_nacrs_flg_1_SL_corp_ids = ed_nacrs_flg_1_SL.select('CORP_ID')
los_org_trend_b = los_org_p_val_parms.join(ed_nacrs_flg_1_SL_corp_ids, 'CORP_ID', 'left_anti')
