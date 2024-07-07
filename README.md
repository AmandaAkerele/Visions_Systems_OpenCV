# Get current date
current_date = datetime.now()

# Determine the comparison date as March 31 of the current year
comparison_date = datetime(datetime.now().year, 3, 31)

# Date processing to determine the fiscal years range
if current_date > comparison_date:
    closed_year = current_date.year - 1
else:
    closed_year = current_date.year - 2
