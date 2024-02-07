# Comparing Results 
values_to_replace=[0.0, 'null']
df2=hsp_ind_organization_fact34.applymap(lambda x: np.nan if x in values_to_replace else x)
