from pyspark.sql import SparkSession
import os

# Start Spark session
spark = SparkSession.builder.appName("Single CSV Output").getOrCreate()

# Assuming los_site_22 DataFrame is already loaded and defined
single_part_df = los_site_22.coalesce(1)
single_part_df.write.csv('delete_los_org_22_temp', header=True, mode='overwrite')

# Renaming the file within the directory
directory = 'delete_los_org_22_temp'
new_filename = 'lo_site_22.csv'
for filename in os.listdir(directory):
    if filename.startswith('part') and filename.endswith('.csv'):
        os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))
        break

# Print the contents of the directory to confirm
print("Files in directory after rename:")
print(os.listdir(directory))

# End Spark session
spark.stop()
