# Get current date
current_date = datetime.now()
    # Determine the comparison date as March 31 of the current year
comparison_date = datetime(datetime.now().year, 3, 31)

# # Date processing to determine the fiscal years range
# current_date = datetime.datetime.now()
# comparison_date = datetime.datetime(current_date.year, 3, 31)

if current_date > comparison_date:
    closed_year = current_date.year - 1
else:
    closed_year = current_date.year - 2


    ---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
/tmp/ipykernel_1563/3709755134.py in <cell line: 15>()
     13 
     14 # Get current date
---> 15 current_date = datetime.now()
     16     # Determine the comparison date as March 31 of the current year
     17 comparison_date = datetime(datetime.now().year, 3, 31)

AttributeError: module 'datetime' has no attribute 'now'
