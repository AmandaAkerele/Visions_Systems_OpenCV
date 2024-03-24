import pandas as pd
import numpy as np
from scipy.stats import expon

# ... (Your mapping dictionaries and other code)

# Remove rows with value 999 in any column
EDWT_Indicators = EDWT_Indicators[~EDWT_Indicators.eq(999).any(axis=1)]

# ... (The rest of your code)
