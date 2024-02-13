# Perform the union operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True) \
                                           .unionByName(t4, allowMissingColumns=True) \
                                           .distinct()





Drop duplicates on these pyspark dataframes before joining 
