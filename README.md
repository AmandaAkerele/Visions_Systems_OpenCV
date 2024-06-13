recreate this code below 

#los_peer_aa.show()
    los_peer_aa = los_peer_a.withColumn('ID', when(col('acute_peer_group_code') == 'T', 2)\
                                        .when(col('acute_peer_group_code') == 'H1', 3) .when(col('acute_peer_group_code') == 'H2', 4)\
                                        .when(col('acute_peer_group_code') == 'H3', 5) .otherwise(None) ) 
    
    
     # Rename ID columns to a common name, let's say "ID"
    los_nt = los_nt.withColumn("ORG_ID", lit(1))
    los_prov = los_prov.withColumnRenamed("adm_prov_id", "ORG_ID")
    los_reg = los_reg.withColumnRenamed("adm_region_id", "ORG_ID")
    los_corp_a = los_corp_a.withColumnRenamed("CORP_ID", "ORG_ID").withColumnRenamed("acute_peer_group_code", "reporting_entity_code")
    los_peer= los_peer_aa.withColumnRenamed("ID", "ORG_ID").withColumnRenamed("acute_peer_group_code", "reporting_entity_code")

    # Define the common schema
    common_columns = ["ORG_ID", "SUBMISSION_FISCAL_YEAR","PERCENTILE_90","reporting_entity_code"]

    
     # Select the common columns or add missing columns with null values
    los_nt = los_nt.select([col(c).alias(c) if c in los_nt.columns else lit(None).alias(c) for c in common_columns])
    los_prov = los_prov.select([col(c).alias(c) if c in los_prov.columns else lit(None).alias(c) for c in common_columns])
    los_reg = los_reg.select([col(c).alias(c) if c in los_reg.columns else lit(None).alias(c) for c in common_columns])
    los_corp_a = los_corp_a.select([col(c).alias(c) if c in los_corp_a.columns else lit(None).alias(c) for c in common_columns])
    los_peer = los_peer.select([col(c).alias(c) if c in los_peer.columns else lit(None).alias(c) for c in common_columns])

# Union the DataFrames
    stacked_df = los_nt.unionByName(los_prov).unionByName(los_reg).unionByName(los_corp_a).unionByName(los_peer).orderBy('ORG_ID')

# Show the result
#stacked_df.show()

# Append the processed DataFrame to final_data list
    stacked_all.append(stacked_df)
