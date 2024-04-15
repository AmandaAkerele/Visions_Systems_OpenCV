                                                                            
---------------------------------------------------------------------------
IndexError                                Traceback (most recent call last)
/tmp/ipykernel_3755/1540527245.py in <cell line: 46>()
     46 for group_name in los_org_all_yr_b.select('TIME').distinct().rdd.flatMap(lambda x: x).collect():
     47     group_data = los_org_all_yr_b.filter(F.col('TIME') == group_name)
---> 48     all_results.append(perform_ols(group_data))
     49 
     50 # Convert results to DataFrame

/tmp/ipykernel_3755/1540527245.py in perform_ols(group_data)
     28     conf_int = results.conf_int(alpha=0.05)
     29     l95b = conf_int[0][1]  # Lower bound for 'time'
---> 30     u95b = conf_int[1][1]  # Upper bound for 'time'
     31 
     32     results_dict = {

IndexError: index 1 is out of bounds for axis 0 with size 1
