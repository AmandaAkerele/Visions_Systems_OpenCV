
    # List of facility with UCC records

    ucc_records = ed_nodup_nosb_22.filter(ed_nodup_nosb_22.amcare_type_code == '11')
    
    ucc_fac = ucc_records.select('org_id', 'FACILITY_AM_CARE_NUM').dropDuplicates()
    
    numerator = ucc_records.groupby("org_id", "FACILITY_AM_CARE_NUM").count().withColumnRenamed('count', 'numerator')
    denominator = ed_nodup_nosb_22.groupby("ORG_ID", "FACILITY_AM_CARE_NUM").count().withColumnRenamed('count', 'denominator').select('ORG_ID', 'denominator')
    
    ucc_fac_count_1 = numerator.join(denominator, ['org_id'], "left")
    ucc_fac_count = ucc_fac_count_1.withColumn('ucc_pct', (ucc_fac_count_1.numerator/ucc_fac_count_1.denominator)*100)
