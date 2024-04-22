# Coalesce the DataFrame to 1 partition
single_part_df = los_site_22.coalesce(1)

# Write to CSV; this will create a folder with a single part-file
single_part_df.write.csv('delete_los_org_22_temp', header=True, mode='overwrite')


# Coalesce the DataFrame to one partition
los_site_22.coalesce(1).write.option("header", "true").csv("/path/to/temp_delete_los_org_22", mode="overwrite")
