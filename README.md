It looks like you are determining the fiscal year based on the current date. To apply this logic to your DataFrame and filter for the year 2022, you'll need to ensure you have a date column in your DataFrame. Then you can create a column representing the fiscal year and filter based on that.

Here is a step-by-step example:

1. Check the schema of your DataFrame.
2. Extract or create a column representing the fiscal year.
3. Filter the DataFrame for the year 2022.

Here’s how you can do it in PySpark:

```python
from pyspark.sql.functions import year, col
from datetime import datetime

# Get current date
current_date = datetime.now()

# Determine the comparison date as March 31 of the current year
comparison_date = datetime(datetime.now().year, 3, 31)

# Date processing to determine the fiscal years range
if current_date > comparison_date:
    closed_year = current_date.year - 1
else:
    closed_year = current_date.year - 2

# Assuming 'date' is the name of the date column in your DataFrame
# First, extract the year from the date column
tpia_supp_org_ucc_22_with_year = tpia_supp_org_ucc_22.withColumn('year', year(tpia_supp_org_ucc_22['date']))

# Filter the DataFrame for the year 2022
tpia_supp_org_ucc_22_filtered = tpia_supp_org_ucc_22_with_year.filter(col('year') == 2022)

# Show the filtered DataFrame
tpia_supp_org_ucc_22_filtered.show()
```

If your DataFrame doesn't have a column named `date`, replace `'date'` with the actual name of your date column.

Make sure the date column is in a format that PySpark can understand (e.g., `yyyy-MM-dd`). If it's not, you might need to convert it first. Let me know if you need further assistance!
