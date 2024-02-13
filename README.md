# Drop specific columns
tmp_ed_facility_org_a = tmp_ed_facility_org_a.dropDuplicates(['SITE_ID', 'CORP_ID', 'REGION_ID', 'REGION_NAME'])
