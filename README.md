from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification of LogisticRegression
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("Logistic Regression Analysis").getOrCreate()

# Assuming 'los_org_all_yr_b' is already defined as a PySpark DataFrame
# Ensure the 'PERCENTILE_90' (label) and 'TIME' (feature) columns are cast to the correct types
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('int'))  # Casting label to int
los_org_all_yr_b = los_org_all_yr_b.withColumn('TIME', F.col('TIME').cast('float'))  # Ensuring feature is float

# Drop any rows where 'PERCENTILE_90' or 'TIME' might be NaN
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Function to perform logistic regression and return results
def perform_logistic(group_data):
    # Make sure the 'TIME' column is of type float (redundant here but safe)
    group_data = group_data.withColumn('TIME', F.col('TIME').cast('float'))
    
    # Create a VectorAssembler with the correctly typed column
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    data = assembler.transform(group_data)
    
    # Create and fit the logistic model
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    model = lr.fit(data)

    # Extracting coefficients and computing metrics
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

# Collecting results for each CORP_ID group
all_results = []
corp_ids = los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect()

for corp_id in corp_ids:
    group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == corp_id)
    result = perform_logistic(group_data)
    all_results.append(result)

# Convert results to a DataFrame and display
los_org_trend = spark.createDataFrame(all_results)
los_org_trend.show()  # Displaying results to verify

# This is a common way to finalize the script and handle potential output for further processing or checking
if los_org_trend.rdd.isEmpty():
    print("No results were generated - check your data and model configuration.")
else:
    print("Analysis completed successfully - results are shown above.")
