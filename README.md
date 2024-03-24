import pandas as pd
import numpy as np
from scipy.stats import expon

# ... (Your mapping dictionaries and other code)

# Remove rows where '999' appears in 'metric_descriptor_code' or 'missing_reason_code'
EDWT_Indicators = EDWT_Indicators[(EDWT_Indicators['metric_descriptor_code'] != 999) & (EDWT_Indicators['missing_reason_code'] != '999')]

# ... (The rest of your code)
