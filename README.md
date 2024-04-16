from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("Refactored PySpark Logistic Regression").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.select(
    F.col('CORP_ID'),
    F.col('PERCENTILE_90').cast('float').alias('PERCENTILE_90'),
    F.col('TIME').cast('float').alias('TIME')
)

# Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Aggregate data to create a feature column and fit a logistic regression model in a pipeline
def logistic_regression_pipeline(data):
    assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="PERCENTILE_90", maxIter=10)
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(data)
    return model

# Use Window to partition by 'CORP_ID' and apply the logistic regression pipeline
from pyspark.sql.window import Window
windowSpec = Window.partitionBy('CORP_ID')

# We perform logistic regression per 'CORP_ID' and collect models
models = los_org_all_yr_b.groupBy('CORP_ID').applyInPandas(logistic_regression_pipeline, schema=los_org_all_yr_b.schema)

# Extract necessary data from models (This part is illustrative; actual implementation may need adjustment based on the model's methods and attributes)
def extract_model_data(model):
    return {
        'CORP_ID': model.uid,  # Example to get CORP_ID, replace with actual method to retrieve ID
        'Coefficients': model.coefficients.tolist(),
        'Intercept': model.intercept,
        'TValues': model.summary.tValues,
        'PValues': model.summary.pValues
    }

# Convert models to a DataFrame if not already one
model_data = spark.createDataFrame([extract_model_data(m) for m in models.collect()])

# Process and analyze results as per business logic (simplified here)
results = model_data.withColumn('Significant', F.col('PValues') < 0.05)

# Example transformation and action based on model results
significant_results = results.filter('Significant').select('CORP_ID', 'Coefficients')
non_significant_results = results.filter(~F.col('Significant')).select('CORP_ID', 'Intercept')

# Continue with business logic to handle results
# (The below is a placeholder to show how you might proceed)
final_results = significant_results.join(non_significant_results, 'CORP_ID', 'outer')



or 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder.appName("Refactored PySpark Linear Regression").getOrCreate()

# Assuming 'los_org_all_yr_b' is a PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.select(
    F.col('CORP_ID'),
    F.col('PERCENTILE_90').cast('float').alias('PERCENTILE_90'),
    F.col('TIME').cast('float').alias('TIME')
)

# Drop rows where either 'PERCENTILE_90' or 'TIME' is NaN
los_org_all_yr_b = los_org_all_yr_b.dropna(subset=['PERCENTILE_90', 'TIME'])

# Set up the DataFrame for Linear Regression
assembler = VectorAssembler(inputCols=["TIME"], outputCol="features")
data = assembler.transform(los_org_all_yr_b)
data = data.select(F.col('CORP_ID'), F.col('PERCENTILE_90').alias('label'), F.col('features'))

# Define Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=10, regParam=0.3)

# Fit the model
lr_model = lr.fit(data)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lr_model.coefficients))
print("Intercept: %s" % str(lr_model.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# Optionally, storing predictions
predictions = lr_model.transform(data)
predictions.select("CORP_ID", "prediction", "label", "features").show(5)

# If further group-wise analysis needed, could group by 'CORP_ID' and calculate stats per group
grouped_data = predictions.groupBy("CORP_ID").agg(
    F.avg('prediction').alias('avg_prediction'),
    F.stddev('prediction').alias('std_deviation'),
    F.max('prediction').alias('max_prediction'),
    F.min('prediction').alias('min_prediction')
)

grouped_data.show()

# The model can also be used for various other analysis, like identifying trends per 'CORP_ID'


or 

pip install statsmodels pandas


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
import pandas as pd
import statsmodels.api as sm

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkStatsModelsIntegration").getOrCreate()

# Assuming 'los_org_all_yr_b' is a pre-defined PySpark DataFrame
# Convert columns to numeric, just in case they are not
los_org_all_yr_b = los_org_all_yr_b.select(
    col('CORP_ID'),
    col('PERCENTILE_90').cast('float').alias('PERCENTILE_90'),
    col('TIME').cast('float').alias('TIME')
).dropna(subset=['PERCENTILE_90', 'TIME'])

# Define a Pandas UDF for performing OLS regression
@pandas_udf("CORP_ID string, slope float, intercept float, r_squared float", PandasUDFType.GROUPED_MAP)
def ols_regression(pdf):
    # Use statsmodels to fit a regression model
    X = sm.add_constant(pdf['TIME'])  # adding a constant
    model = sm.OLS(pdf['PERCENTILE_90'], X).fit()
    return pd.DataFrame({
        'CORP_ID': [pdf['CORP_ID'].iloc[0]],
        'slope': [model.params['TIME']],
        'intercept': [model.params['const']],
        'r_squared': [model.rsquared]
    })

# Apply the OLS Pandas UDF to the DataFrame grouped by 'CORP_ID'
regression_results = los_org_all_yr_b.groupby('CORP_ID').apply(ols_regression)

# Show the results
regression_results.show()

# Stop the Spark session
spark.stop()
