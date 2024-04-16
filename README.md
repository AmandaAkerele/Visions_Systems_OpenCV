from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("Convert to PySpark").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.withColumn('PERCENTILE_90', F.col('PERCENTILE_90').cast('float')) \
                                   .withColumn('TIME', F.col('TIME').cast('float'))

# Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN after the conversion
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Function to perform logistic regression and return results
def perform_logistic(group_data):
    # Create a vector assembler
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    data = assembler.transform(group_data)

    # Create and fit the model
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    model = lr.fit(data)

    # Extract coefficients and ensure there are enough data points
    coef = model.coefficients[0] if model.coefficients.size > 0 else 0
    residuals = model.summary.residuals.rdd.map(lambda x: x[0]**2).collect()
    n = len(residuals)
    mse = sum(residuals) / n if n > 0 else 0
    se = np.sqrt(mse / n) if n > 0 else 0
    
    t_val = coef / se if se != 0 else 0
    p_val = model.summary.pValues[0] if model.summary.pValues else 1
    l95b = coef - 1.96 * se  # Lower bound for 'time'
    u95b = coef + 1.96 * se  # Upper bound for 'time'

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
for corp_id_row in los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect():
    group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == corp_id_row)
    all_results.append(perform_logistic(group_data))

# Convert results to DataFrame
los_org_trend = spark.createDataFrame(all_results)

# Additional DataFrame transformations as per your processing needs
los_org_trend.show()
