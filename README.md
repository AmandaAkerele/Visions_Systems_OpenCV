from pyspark.sql.functions import col, when

# Assuming organization_data is a PySpark DataFrame read from SAS data

# Filter the DataFrame for ORGANIZATION_ID values in (2, 3, 4, 5)
filtered_data = organization_data.filter(col('ORGANIZATION_ID').isin([2, 3, 4, 5]))

# Apply the mapping to create the 'peer_code' column using when function
filtered_data = filtered_data.withColumn(
    'peer_code',
    when(col('ORGANIZATION_ID') == 2, 'T')
    .when(col('ORGANIZATION_ID') == 3, 'H1')
    .when(col('ORGANIZATION_ID') == 4, 'H2')
    .when(col('ORGANIZATION_ID') == 5, 'H3')
)

# Select and rename the desired columns
peer_desc_df = filtered_data.select(
    col('ORGANIZATION_ID').alias('peer_id'), 
    col('peer_code'), 
    col('ORGANIZATION_NAME_E_DESC').alias('peer_desc')
)

# Show the resulting DataFrame
peer_desc_df.show()




or


from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Assuming organization_data is a PySpark DataFrame read from SAS data

# Create a dictionary to map ORGANIZATION_ID values to peer codes
id_to_peer_code = {2: 'T', 3: 'H1', 4: 'H2', 5: 'H3'}

# Define a UDF to map ORGANIZATION_ID to peer codes
map_id_to_peer_code_udf = udf(lambda id: id_to_peer_code[id], StringType())

# Filter the organization_data DataFrame for ORGANIZATION_ID values in (2, 3, 4, 5)
filtered_data = organization_data.filter(col('ORGANIZATION_ID').isin([2, 3, 4, 5]))

# Apply the mapping to create the 'peer_code' column
filtered_data = filtered_data.withColumn('peer_code', map_id_to_peer_code_udf(col('ORGANIZATION_ID')))

# Select and rename the desired columns
peer_desc_df = filtered_data.select(
    col('ORGANIZATION_ID').alias('peer_id'), 
    col('peer_code'), 
    col('ORGANIZATION_NAME_E_DESC').alias('peer_desc')
)

# Show the resulting DataFrame
peer_desc_df.show()



