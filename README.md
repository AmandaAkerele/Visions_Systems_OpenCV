solve error 

---------------------------------------------------------------------------
IndexError                                Traceback (most recent call last)
/tmp/ipykernel_324/50004701.py in <cell line: 27>()
     27 for group_name in los_org_all_yr_b.select('CORP_ID').distinct().rdd.flatMap(lambda x: x).collect():
     28     group_data = los_org_all_yr_b.filter(F.col('CORP_ID') == group_name)
---> 29     all_results.extend(perform_ols(group_data))
     30 
     31 # Convert to DataFrame and analyze results

/tmp/ipykernel_324/50004701.py in perform_ols(data)
     15     for param_type, value in zip(['PARAMS', 'STDERR', 'T', 'PVALUE', 'L95B', 'U95B'],
     16                                  [model.params[1], model.bse[1], model.tvalues[1], model.pvalues[1],
---> 17                                   conf_int['const'][0], conf_int['const'][1]]):
     18         results.append({'CORP_ID': data.select('CORP_ID').first()[0], '_TYPE_': param_type, 'VALUE': value})
     19 

IndexError: only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or boolean arrays are valid indices

