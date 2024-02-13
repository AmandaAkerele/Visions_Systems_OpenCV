# Dropping duplicates in each DataFrame
tmp_ed_facility_org_a = tmp_ed_facility_org_a.dropDuplicates()
t3 = t3.dropDuplicates()
t4 = t4.dropDuplicates()

# Perform the union operation
tmp_ed_facility_org = tmp_ed_facility_org_a.unionByName(t3, allowMissingColumns=True) \
                                           .unionByName(t4, allowMissingColumns=True) \
                                           .distinct()
