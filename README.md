what changed in this code 
  # Stack all processed DataFrames using reduce and unionAll()
    final_set = reduce(lambda x, y: x.unionAll(y), stacked_all)
    
    final_set=final_set.withColumn("reporting_entity_code_text", col('ORG_ID').cast("string"))
    final_set=final_set.drop("reporting_entity_code").drop('ORG_ID')
    final_set = final_set.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    
    
    final_set_df = final_set.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year")))\
    .withColumn("reporting_entity_type_code", lit("ORG"))\
    .withColumn("indicator_code", lit("810")).withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A"))\
    .withColumn("metric_descriptor_group_code", lit(" ")).withColumn("metric_descriptor_code", lit(" "))\
    .withColumn("missing_reason_code", lit(" "))\
    .withColumn("breakdown_value_code_l2", lit("N/A")).withColumn("metric_result", col("PERCENTILE_90")).withColumn("public_metric_result", col("PERCENTILE_90"))
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    final_set_df = final_set_df.select(*desired_columns)
    #replace org_id for peer groups
    final_set_df = final_set_df.withColumn("reporting_entity_code", when(col("reporting_entity_code") == '2', "HPEER_T"). when(col("reporting_entity_code") == '3', "HPEER_H1").\
                             when(col("reporting_entity_code") == '4', "HPEER_H2"). when(col("reporting_entity_code") == '5', "HPEER_H3")\
                                 .otherwise(col("reporting_entity_code"))).withColumn("reporting_entity_type_code",when(col("reporting_entity_code")\
                                    .isin(["HPEER_T", "HPEER_H1", "HPEER_H2","HPEER_H3"]), "ORGCHAR").otherwise(col("reporting_entity_type_code"))
    )
    # Show the new DataFrame to verify the column order
    #final_set_df.printSchema()
    #final_set_df.printSchema()
    
    # Define the window specification to partition by ORG_ID and order by year
    windowSpec = Window.partitionBy("CORP_ID").orderBy("SUBMISSION_FISCAL_YEAR")
    #get the corp all from stacked dataframes
    corp_all = reduce(lambda x, y: x.unionAll(y), los_corp_all)
    # Add a column to flag the rows that are in the PERCENTILE_90
    corp_all = corp_all.withColumn("in_percentile_90", F.when(F.col("PERCENTILE_90"). isNotNull(), 1).otherwise(0))
    #corp_all.show(5)
    # Aggregate to count the number of years in PERCENTILE_90 for each ORG_ID
    corp_count = corp_all.groupBy("CORP_ID").agg(F.sum("in_percentile_90").alias("years_in_percentile_90"))
    #df_count.show(5)
    # Filter ORG_IDs with at least 3 years in PERCENTILE_90
    corp_filtered = corp_count.filter(F.col("years_in_percentile_90") >= 3)
    # Join back with the original DataFrame to get the full records for these ORG_IDs
    corp_comp = corp_all.join(corp_filtered, on="CORP_ID", how="inner").filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    corp_trend = corp_all.join(corp_filtered, on="CORP_ID", how="inner") \
        .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(str(int(closed_year)-1), str(int(closed_year)-2), str(int(closed_year)-3))) 
    # Drop the temporary column   str(closed_year)
    #los_org_3x3 = los_org_3x3.drop("in_percentile_90", "years_in_percentile_90")
    # Show the result
    #corp_comp.show()
    
    # Define the window specification to partition by ORG_ID and order by year
    windowSpec = Window.partitionBy("adm_region_id").orderBy("SUBMISSION_FISCAL_YEAR")
    #get the reg all data set from stacked 
    reg_all = reduce(lambda x, y: x.unionAll(y), los_reg_all)
    # Add a column to flag the rows that are in the PERCENTILE_90
    reg_all = reg_all.withColumn("in_percentile_90", F.when(F.col("PERCENTILE_90"). isNotNull(), 1).otherwise(0))
    #corp_all.show(5)
    # Aggregate to count the number of years in PERCENTILE_90 for each ORG_ID
    reg_count = reg_all.groupBy("adm_region_id").agg(F.sum("in_percentile_90").alias("years_in_percentile_90"))
    #df_count.show(5)
    # Filter ORG_IDs with at least 3 years in PERCENTILE_90
    reg_filtered = reg_count.filter(F.col("years_in_percentile_90") >= 3)
    # Join back with the original DataFrame to get the full records for these ORG_IDs
    reg_comp = reg_all.join(reg_filtered, on="adm_region_id", how="inner").filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    reg_trend = reg_all.join(reg_filtered, on="adm_region_id", how="inner") \
        .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(str(int(closed_year)-1), str(int(closed_year)-2), str(int(closed_year)-3)))  
    # Drop the temporary column
    #los_org_3x3 = los_org_3x3.drop("in_percentile_90", "years_in_percentile_90")
    # Show the result
    #reg_comp.count()
    #create baseline datasets
    los_reg_ta_final = reg_all.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    
    los_corp_ta_final = corp_all.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    
    
    los_peer_base80= los_corp_ta_final.groupBy('SUBMISSION_FISCAL_YEAR','acute_peer_group_code') \
                                  .agg(F.percentile_approx('PERCENTILE_90', 0.8).alias('PERCENTILE_80'))
    #los_peer_base80.show()
    los_peer_base20= los_corp_ta_final.groupBy('SUBMISSION_FISCAL_YEAR','acute_peer_group_code') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.2).alias('PERCENTILE_20'))
    selected = los_peer_base80.select("acute_peer_group_code", "PERCENTILE_80")
    # Performing the left join 
    los_peer_base = los_peer_base20.join(selected, on="acute_peer_group_code", how="left") 
    los_reg_base80= los_reg_ta_final.groupBy('SUBMISSION_FISCAL_YEAR') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.8).alias('PERCENTILE_80'))
    #los_reg_base80.show()
    los_reg_base20= los_reg_ta_final.groupBy('SUBMISSION_FISCAL_YEAR') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.2).alias('PERCENTILE_20'))
    selected = los_reg_base80.select("SUBMISSION_FISCAL_YEAR","PERCENTILE_80")
    # Performing the left join 
    los_reg_base = los_reg_base20.join(selected, on="SUBMISSION_FISCAL_YEAR", how="left") 
    #los_peer_base.show()
    #los_reg_base.show()
    # Create comparison datasets
    # Alias the DataFrames for clarity
    corp_comp_alias = corp_comp.alias("los_corp_a")
    los_peer_base_alias = los_peer_base.alias("los_peer_base")
    
    # Perform the join and select the required columns
    result_corp = corp_comp_alias.join(los_peer_base_alias, on="acute_peer_group_code", how="inner") \
                                  .select(
                                      col("los_corp_a.CORP_ID"),
                                      col("los_corp_a.SUBMISSION_FISCAL_YEAR"),
                                      col("los_corp_a.PERCENTILE_90"),
                                      col("los_corp_a.acute_peer_group_code"),
                                      col("los_peer_base.PERCENTILE_20"),
                                      col("los_peer_base.PERCENTILE_80")
                                  )
        
    # Uncomment this line to show the result
    # result_corp.show()
    result_corp_a = result_corp.withColumn("COMPARE_IND_CODE", lit(" ")) \
                                 .withColumn("metric_descriptor_group_code", lit(" ")) \
                                 .withColumn("missing_reason_code", lit(" ")) \
                                 .withColumn("metric_descriptor_code", lit(" ")) \
                                 .withColumn("COMPARE_IND_CODE", when(col("percentile_90") < col("percentile_20"), "001")
                                             .when((col("percentile_90") >= col("percentile_20")) & (col("percentile_90") <= col("percentile_80")), "002")
                                             .when(col("percentile_90") > col("percentile_80"), "003")
                                             .otherwise(" ")) \
                                 .withColumn("metric_descriptor_code", when(col("COMPARE_IND_CODE") == "001", "Above")
                                             .when(col("COMPARE_IND_CODE") == "002", "Same")
                                             .when(col("COMPARE_IND_CODE") == "003", "Below")
                                             .otherwise(" "))\
                                 .withColumn("metric_descriptor_group_code", when(col("COMPARE_IND_CODE") .isNotNull(), "PerformanceComparison")
                                             .otherwise(" "))
     #we do not do prov level coparisons   
    #result_prov = los_prov.withColumn("COMPARE_IND_CODE", lit("999")) \
     #                           .withColumn("COMPARE_IND_E_DESC", lit("NA"))
        
    result_reg = reg_comp.join(los_reg_base, on="SUBMISSION_FISCAL_YEAR", how="inner").select("*")
    result_reg_a = result_reg.withColumn("COMPARE_IND_CODE", lit(" ")) \
                                 .withColumn("metric_descriptor_group_code", lit(" ")) \
                                 .withColumn("missing_reason_code", lit(" ")) \
                                 .withColumn("metric_descriptor_code", lit(" ")) \
                                 .withColumn("COMPARE_IND_CODE", when(col("percentile_90") < col("percentile_20"), "001")
                                             .when((col("percentile_90") >= col("percentile_20")) & (col("percentile_90") <= col("percentile_80")), "002")
                                             .when(col("percentile_90") > col("percentile_80"), "003")
                                             .otherwise(" ")) \
                                 .withColumn("metric_descriptor_code", when(col("COMPARE_IND_CODE") == "001", "Above")
                                             .when(col("COMPARE_IND_CODE") == "002", "Same")
                                             .when(col("COMPARE_IND_CODE") == "003", "Below")
                                             .otherwise(" "))\
                                .withColumn("metric_descriptor_group_code", when(col("COMPARE_IND_CODE") .isNotNull(), "PerformanceComparison")
                                             .otherwise(" "))
    
    
    corp_comp_final = result_corp_a.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("SUBMISSION_FISCAL_YEAR"))).withColumnRenamed("CORP_ID", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG ")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0))\
    .drop("PERCENTILE_20").drop("PERCENTILE_80").drop("acute_peer_group_code").drop("COMPARE_IND_CODE").drop("PERCENTILE_90").drop("SUBMISSION_FISCAL_YEAR")
    corp_comp_final=corp_comp_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    corp_comp_final=corp_comp_final.drop("reporting_entity_code").drop('ORG_ID')
    corp_comp_final = corp_comp_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    corp_comp_final =corp_comp_final.select(*desired_columns)
    
    #corp_comp_final.printSchema()
    
    reg_comp_final = result_reg_a.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("SUBMISSION_FISCAL_YEAR"))).withColumnRenamed("adm_region_id", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG ")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0))\
    .drop("PERCENTILE_20").drop("PERCENTILE_80").drop("acute_peer_group_code").drop("COMPARE_IND_CODE").drop("PERCENTILE_90").drop("SUBMISSION_FISCAL_YEAR")
    reg_comp_final=reg_comp_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    reg_comp_final=reg_comp_final.drop("reporting_entity_code").drop('ORG_ID')
    reg_comp_final = reg_comp_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    reg_comp_final =reg_comp_final.select(*desired_columns)
    
    #reg_comp_final.printSchema()
    #create trending dataset
    # Define a UDF for performing linear regression using scipy
    def perform_regression(times, percentiles):
            times_float = np.array(times, dtype=np.float64)
            percentiles_float = np.array(percentiles, dtype=np.float64)
            if len(times_float) <= 1 or len(percentiles_float) <= 1:
                return [None, None, None, None]
            slope, intercept, r_value, p_value, std_err = linregress(times_float, percentiles_float)
            return [float(slope), float(intercept), float(p_value), float(std_err)]
     
    schema = ArrayType(FloatType())
    regression_udf = udf(perform_regression, schema)
     
    # Apply the UDF to compute regression parameters
    grouped_data_corp =  corp_trend.groupBy("CORP_ID").agg(
        collect_list(col("SUBMISSION_FISCAL_YEAR")).alias("times"),
        collect_list(col("PERCENTILE_90")).alias("percentiles")
    )
    los_corp_trend_a = grouped_data_corp.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))
     
    # Expand the results into separate columns
    merged = los_corp_trend_a.select(
        "CORP_ID",
        col("regression_results").getItem(0).alias("slope"),
        col("regression_results").getItem(1).alias("intercept"),
        col("regression_results").getItem(2).alias("p_value"),
        col("regression_results").getItem(3).alias("std_err")
    )
     
    # Define improvement indicators
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", lit("002"))
    merged = merged.withColumn("metric_descriptor_code", lit("NoChange"))
    merged = merged.withColumn("missing_reason_code", lit(" "))
     
    # Applying the logic from the provided method
    mask = (merged['p_value'] < 0.05) & (merged['slope'] > 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("003")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_code", when(mask, lit("Weaken")).otherwise(col("metric_descriptor_code")))
     
    mask = (merged['p_value'] < 0.05) & (merged['slope'] < 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("001")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_group_code", when(col("IMPROVEMENT_IND_CODE") .isNotNull(), "PerformanceTrend").otherwise(" "))
    corp_trend_df = merged.withColumn("metric_descriptor_code", when(mask, lit("Improving")).otherwise(col("metric_descriptor_code")))
    corp_trend_df = corp_trend_df.select("CORP_ID","IMPROVEMENT_IND_CODE","metric_descriptor_code","metric_descriptor_group_code","missing_reason_code")
    corp_trend_df=corp_trend_df.withColumn("SUBMISSION_FISCAL_YEAR", lit(str(closed_year-1)))
    corp_trend_final = corp_trend_df.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year"))).withColumnRenamed("CORP_ID", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG ")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0)).drop("SUBMISSION_FISCAL_YEAR").drop("IMPROVEMENT_IND_CODE")
    corp_trend_final=corp_trend_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    corp_trend_final=corp_trend_final.drop("reporting_entity_code")
    corp_trend_final = corp_trend_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    corp_trend_final = corp_trend_final.select(*desired_columns)
    
    # Show the new DataFrame to verify the column order
    #corp_trend_final.show()
    #corp_trend_final.printSchema()
    
    grouped_data_reg =  reg_trend.groupBy("adm_region_id").agg(
        collect_list(col("SUBMISSION_FISCAL_YEAR")).alias("times"),
        collect_list(col("PERCENTILE_90")).alias("percentiles")
    )
    los_reg_trend_a = grouped_data_reg.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))
     
    # Expand the results into separate columns
    merged = los_reg_trend_a.select(
        "adm_region_id",
        col("regression_results").getItem(0).alias("slope"),
        col("regression_results").getItem(1).alias("intercept"),
        col("regression_results").getItem(2).alias("p_value"),
        col("regression_results").getItem(3).alias("std_err")
    )
     
    # Define improvement indicators
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", lit("002"))
    merged = merged.withColumn("metric_descriptor_code", lit("NoChange"))
    merged = merged.withColumn("missing_reason_code", lit(" "))
     
    # Applying the logic from the provided method
    mask = (merged['p_value'] < 0.05) & (merged['slope'] > 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("003")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_code", when(mask, lit("Weaken")).otherwise(col("metric_descriptor_code")))
     
    mask = (merged['p_value'] < 0.05) & (merged['slope'] < 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("001")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_group_code", when(col("IMPROVEMENT_IND_CODE") .isNotNull(), "PerformanceTrend").otherwise(" "))
    
    reg_trend_df = merged.withColumn("metric_descriptor_code", when(mask, lit("Improving")).otherwise(col("metric_descriptor_code")))
    reg_trend_df = reg_trend_df.select("adm_region_id","IMPROVEMENT_IND_CODE","metric_descriptor_code","metric_descriptor_group_code","missing_reason_code")
    reg_trend_df=reg_trend_df.withColumn("SUBMISSION_FISCAL_YEAR", lit(str(closed_year-1)))
    reg_trend_final = reg_trend_df.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year"))).withColumnRenamed("adm_region_id", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG ")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0)).drop("SUBMISSION_FISCAL_YEAR").drop("IMPROVEMENT_IND_CODE")
    
    reg_trend_final=reg_trend_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    reg_trend_final=reg_trend_final.drop("reporting_entity_code")
    reg_trend_final = reg_trend_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    reg_trend_final = reg_trend_final.select(*desired_columns)
    
    # Show the new DataFrame to verify the column order
    #reg_trend_final.show()
    #reg_trend_final.printSchema()
    data_810 = final_set_df.unionByName(reg_trend_final).unionByName(corp_trend_final).unionByName(corp_comp_final)\
    .unionByName(reg_comp_final).orderBy('reporting_period_code','reporting_entity_code')
    data_submission_810 = data_810.filter(col("reporting_entity_code").isNotNull())


    # Stack all dataframes using reduce and unionAll()
    los_supp_corp = reduce(lambda x, y: x.unionAll(y), los_supp_orgs)
    los_supp_reg = reduce(lambda x, y: x.unionAll(y), los_supp_regs)
    los_supp_peer = reduce(lambda x, y: x.unionAll(y), los_supp_peers)
    #reg_all = reduce(lambda x, y: x.unionAll(y), los_reg_all)
    #corp_all = reduce(lambda x, y: x.unionAll(y), los_corp_all)
    ucc_all_corp = reduce(lambda x, y: x.unionAll(y), ucc_all) 
    ps_all_corp = reduce(lambda x, y: x.unionAll(y), ps_all)
    
    # Save intermediate or additional data for business area usage/reference (e.g.: for external data requests, DQ verification)
    intermediate_data_dict["data_sub_810" + str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = data_submission_810
    intermediate_data_dict["los_corp_all"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = corp_all
    intermediate_data_dict["los_reg_all"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = reg_all
    intermediate_data_dict["los_supp_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_corp
    intermediate_data_dict["los_supp_peer"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_peer
    intermediate_data_dict["los_supp_reg"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_reg
    intermediate_data_dict["ps_all_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = ps_all_corp
    intermediate_data_dict["ucc_all_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = ucc_all_corp

    
    # Generate an output_df in a common format for your project
    # For indicator automation, output_df shares the same layout and requirements as shallow slice
    output_df = data_submission_810.select('*')
    
    # Return both output_df and dictionary of intermediate dataframes
    return output_df, intermediate_data_dict



this is the new code below: what new thing was added to the new code below that is not in the code above:
 # Stack all processed DataFrames using reduce and unionAll()
    final_set = reduce(lambda x, y: x.unionAll(y), stacked_all)
    
    final_set=final_set.withColumn("reporting_entity_code_text", col('ORG_ID').cast("string"))
    final_set=final_set.drop("reporting_entity_code").drop('ORG_ID')
    final_set = final_set.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    
    
    final_set_df = final_set.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year")))\
    .withColumn("reporting_entity_type_code", lit("ORG"))\
    .withColumn("indicator_code", lit("810")).withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A"))\
    .withColumn("metric_descriptor_group_code", lit(None).cast(StringType()))\
    .withColumn("metric_descriptor_code", lit(" "))\
    .withColumn('missing_reason_code' , lit(None).cast(StringType()))\
    .withColumn("breakdown_type_code_l3", lit("N/A")).withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("segment_type_code", lit("N/A")).withColumn("segment_value_code", lit("N/A")).withColumn("reporting_type_code", lit("Pub"))\
    .withColumn("breakdown_value_code_l2", lit("N/A")).withColumn("metric_result", col("PERCENTILE_90")).withColumn("public_metric_result", col("PERCENTILE_90"))
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "segment_type_code",
        "segment_value_code",
        "reporting_type_code",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    final_set_df = final_set_df.select(*desired_columns)
    #replace org_id for peer groups
    final_set_df = final_set_df.withColumn("reporting_entity_code", when(col("reporting_entity_code") == '2', "HPEER_T"). when(col("reporting_entity_code") == '3', "HPEER_H1").\
                             when(col("reporting_entity_code") == '4', "HPEER_H2"). when(col("reporting_entity_code") == '5', "HPEER_H3")\
                                 .otherwise(col("reporting_entity_code"))).withColumn("reporting_entity_type_code",when(col("reporting_entity_code")\
                                    .isin(["HPEER_T", "HPEER_H1", "HPEER_H2","HPEER_H3"]), "ORGCHAR").otherwise(col("reporting_entity_type_code"))
    )
    # Show the new DataFrame to verify the column order
    #final_set_df.printSchema()
    #final_set_df.printSchema()
    
    # Define the window specification to partition by ORG_ID and order by year
    windowSpec = Window.partitionBy("CORP_ID").orderBy("SUBMISSION_FISCAL_YEAR")
    
    # Add a column to flag the rows that are in the PERCENTILE_90
    corp_all = corp_all.withColumn("in_percentile_90", F.when(F.col("PERCENTILE_90"). isNotNull(), 1).otherwise(0))
    #corp_all.show(5)
    # Aggregate to count the number of years in PERCENTILE_90 for each ORG_ID
    corp_count = corp_all.groupBy("CORP_ID").agg(F.sum("in_percentile_90").alias("years_in_percentile_90"))
    #df_count.show(5)
    # Filter ORG_IDs with at least 3 years in PERCENTILE_90
    corp_filtered = corp_count.filter(F.col("years_in_percentile_90") >= 3)
    # Join back with the original DataFrame to get the full records for these ORG_IDs
    corp_comp = corp_all.join(corp_filtered, on="CORP_ID", how="inner").filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    corp_trend = corp_all.join(corp_filtered, on="CORP_ID", how="inner") \
        .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(str(int(closed_year)-1), str(int(closed_year)-2), str(int(closed_year)-3))) 
    # Drop the temporary column   str(closed_year)
    #los_org_3x3 = los_org_3x3.drop("in_percentile_90", "years_in_percentile_90")
    # Show the result
    #corp_comp.show()
    
    # Define the window specification to partition by ORG_ID and order by year
    windowSpec = Window.partitionBy("adm_region_id").orderBy("SUBMISSION_FISCAL_YEAR")
    
    # Add a column to flag the rows that are in the PERCENTILE_90
    reg_all = reg_all.withColumn("in_percentile_90", F.when(F.col("PERCENTILE_90"). isNotNull(), 1).otherwise(0))
    #corp_all.show(5)
    # Aggregate to count the number of years in PERCENTILE_90 for each ORG_ID
    reg_count = reg_all.groupBy("adm_region_id").agg(F.sum("in_percentile_90").alias("years_in_percentile_90"))
    #df_count.show(5)
    # Filter ORG_IDs with at least 3 years in PERCENTILE_90
    reg_filtered = reg_count.filter(F.col("years_in_percentile_90") >= 3)
    # Join back with the original DataFrame to get the full records for these ORG_IDs
    reg_comp = reg_all.join(reg_filtered, on="adm_region_id", how="inner").filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    reg_trend = reg_all.join(reg_filtered, on="adm_region_id", how="inner") \
        .filter(F.col("SUBMISSION_FISCAL_YEAR").isin(str(int(closed_year)-1), str(int(closed_year)-2), str(int(closed_year)-3)))  
    # Drop the temporary column
    #los_org_3x3 = los_org_3x3.drop("in_percentile_90", "years_in_percentile_90")
    # Show the result
    #reg_comp.count()
    #create baseline datasets
    los_reg_ta_final = reg_all.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    
    los_corp_ta_final = corp_all.filter(F.col("SUBMISSION_FISCAL_YEAR") == str(closed_year-1))
    
    
    los_peer_base80= los_corp_ta_final.groupBy('SUBMISSION_FISCAL_YEAR','acute_peer_group_code') \
                                  .agg(F.percentile_approx('PERCENTILE_90', 0.8).alias('PERCENTILE_80'))
    #los_peer_base80.show()
    los_peer_base20= los_corp_ta_final.groupBy('SUBMISSION_FISCAL_YEAR','acute_peer_group_code') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.2).alias('PERCENTILE_20'))
    selected = los_peer_base80.select("acute_peer_group_code", "PERCENTILE_80")
    # Performing the left join 
    los_peer_base = los_peer_base20.join(selected, on="acute_peer_group_code", how="left") 
    los_reg_base80= los_reg_ta_final.groupBy('SUBMISSION_FISCAL_YEAR') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.8).alias('PERCENTILE_80'))
    #los_reg_base80.show()
    los_reg_base20= los_reg_ta_final.groupBy('SUBMISSION_FISCAL_YEAR') \
                              .agg(F.percentile_approx('PERCENTILE_90', 0.2).alias('PERCENTILE_20'))
    selected = los_reg_base80.select("SUBMISSION_FISCAL_YEAR","PERCENTILE_80")
    # Performing the left join 
    los_reg_base = los_reg_base20.join(selected, on="SUBMISSION_FISCAL_YEAR", how="left") 
    #los_peer_base.show()
    #los_reg_base.show()
    # Create comparison datasets
    # Alias the DataFrames for clarity
    corp_comp_alias = corp_comp.alias("los_corp_a")
    los_peer_base_alias = los_peer_base.alias("los_peer_base")
    
    # Perform the join and select the required columns
    result_corp = corp_comp_alias.join(los_peer_base_alias, on="acute_peer_group_code", how="inner") \
                                  .select(
                                      col("los_corp_a.CORP_ID"),
                                      col("los_corp_a.SUBMISSION_FISCAL_YEAR"),
                                      col("los_corp_a.PERCENTILE_90"),
                                      col("los_corp_a.acute_peer_group_code"),
                                      col("los_peer_base.PERCENTILE_20"),
                                      col("los_peer_base.PERCENTILE_80")
                                  )
        
    # Uncomment this line to show the result
    # result_corp.show()
    result_corp_a = result_corp.withColumn("COMPARE_IND_CODE", lit(" ")) \
                                 .withColumn("metric_descriptor_group_code", lit(None).cast(StringType())) \
                                 .withColumn('missing_reason_code' , lit(None).cast(StringType())) \
                                 .withColumn("metric_descriptor_code", lit(" ")) \
                                 .withColumn("COMPARE_IND_CODE", when(col("percentile_90") < col("percentile_20"), "001")
                                             .when((col("percentile_90") >= col("percentile_20")) & (col("percentile_90") <= col("percentile_80")), "002")
                                             .when(col("percentile_90") > col("percentile_80"), "003")
                                             .otherwise(" ")) \
                                 .withColumn("metric_descriptor_code", when(col("COMPARE_IND_CODE") == "001", "Above")
                                             .when(col("COMPARE_IND_CODE") == "002", "Same")
                                             .when(col("COMPARE_IND_CODE") == "003", "Below")
                                             .otherwise(" "))\
                                 .withColumn("metric_descriptor_group_code", when(col("COMPARE_IND_CODE") .isNotNull(), "PerformanceComparison")
                                             .otherwise(" "))
     #we do not do prov level coparisons   
    #result_prov = los_prov.withColumn("COMPARE_IND_CODE", lit("999")) \
     #                           .withColumn("COMPARE_IND_E_DESC", lit("NA"))
        
    result_reg = reg_comp.join(los_reg_base, on="SUBMISSION_FISCAL_YEAR", how="inner").select("*")
    result_reg_a = result_reg.withColumn("COMPARE_IND_CODE", lit(" ")) \
                                 .withColumn("metric_descriptor_group_code", lit(None).cast(StringType())) \
                                 .withColumn('missing_reason_code' , lit(None).cast(StringType())) \
                                 .withColumn("metric_descriptor_code", lit(" ")) \
                                 .withColumn("COMPARE_IND_CODE", when(col("percentile_90") < col("percentile_20"), "001")
                                             .when((col("percentile_90") >= col("percentile_20")) & (col("percentile_90") <= col("percentile_80")), "002")
                                             .when(col("percentile_90") > col("percentile_80"), "003")
                                             .otherwise(" ")) \
                                 .withColumn("metric_descriptor_code", when(col("COMPARE_IND_CODE") == "001", "Above")
                                             .when(col("COMPARE_IND_CODE") == "002", "Same")
                                             .when(col("COMPARE_IND_CODE") == "003", "Below")
                                             .otherwise(" "))\
                                .withColumn("metric_descriptor_group_code", when(col("COMPARE_IND_CODE") .isNotNull(), "PerformanceComparison")
                                             .otherwise(" "))
    
    
    corp_comp_final = result_corp_a.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("SUBMISSION_FISCAL_YEAR"))).withColumnRenamed("CORP_ID", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG")).withColumn("indicator_code", lit("810"))\
    .withColumn("breakdown_type_code_l3", lit("N/A")).withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("segment_type_code", lit("N/A")).withColumn("segment_value_code", lit("N/A")).withColumn("reporting_type_code", lit("Pub"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0))\
    .drop("PERCENTILE_20").drop("PERCENTILE_80").drop("acute_peer_group_code").drop("COMPARE_IND_CODE").drop("PERCENTILE_90").drop("SUBMISSION_FISCAL_YEAR")
    corp_comp_final=corp_comp_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    corp_comp_final=corp_comp_final.drop("reporting_entity_code").drop('ORG_ID')
    corp_comp_final = corp_comp_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "segment_type_code",
        "segment_value_code",
        "reporting_type_code",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    corp_comp_final =corp_comp_final.select(*desired_columns)
    
    #corp_comp_final.printSchema()
    
    reg_comp_final = result_reg_a.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("SUBMISSION_FISCAL_YEAR"))).withColumnRenamed("adm_region_id", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0))\
    .withColumn("breakdown_type_code_l3", lit("N/A")).withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("segment_type_code", lit("N/A")).withColumn("segment_value_code", lit("N/A")).withColumn("reporting_type_code", lit("Pub"))\
    .drop("PERCENTILE_20").drop("PERCENTILE_80").drop("acute_peer_group_code").drop("COMPARE_IND_CODE").drop("PERCENTILE_90").drop("SUBMISSION_FISCAL_YEAR")
    reg_comp_final=reg_comp_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    reg_comp_final=reg_comp_final.drop("reporting_entity_code").drop('ORG_ID')
    reg_comp_final = reg_comp_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "segment_type_code",
        "segment_value_code",
        "reporting_type_code",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    reg_comp_final =reg_comp_final.select(*desired_columns)
    
    #reg_comp_final.printSchema()
    #create trending dataset
    # Define a UDF for performing linear regression using scipy
    def perform_regression(times, percentiles):
            times_float = np.array(times, dtype=np.float64)
            percentiles_float = np.array(percentiles, dtype=np.float64)
            if len(times_float) <= 1 or len(percentiles_float) <= 1:
                return [None, None, None, None]
            slope, intercept, r_value, p_value, std_err = linregress(times_float, percentiles_float)
            return [float(slope), float(intercept), float(p_value), float(std_err)]
     
    schema = ArrayType(FloatType())
    regression_udf = udf(perform_regression, schema)
     
    # Apply the UDF to compute regression parameters
    grouped_data_corp =  corp_trend.groupBy("CORP_ID").agg(
        collect_list(col("SUBMISSION_FISCAL_YEAR")).alias("times"),
        collect_list(col("PERCENTILE_90")).alias("percentiles")
    )
    los_corp_trend_a = grouped_data_corp.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))
     
    # Expand the results into separate columns
    merged = los_corp_trend_a.select(
        "CORP_ID",
        col("regression_results").getItem(0).alias("slope"),
        col("regression_results").getItem(1).alias("intercept"),
        col("regression_results").getItem(2).alias("p_value"),
        col("regression_results").getItem(3).alias("std_err")
    )
     
    # Define improvement indicators
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", lit("002"))
    merged = merged.withColumn("metric_descriptor_code", lit("NoChange"))
    merged = merged.withColumn('missing_reason_code' , lit(None).cast(StringType()))
     
    # Applying the logic from the provided method
    mask = (merged['p_value'] < 0.05) & (merged['slope'] > 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("003")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_code", when(mask, lit("Weaken")).otherwise(col("metric_descriptor_code")))
     
    mask = (merged['p_value'] < 0.05) & (merged['slope'] < 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("001")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_group_code", when(col("IMPROVEMENT_IND_CODE") .isNotNull(), "PerformanceTrend").otherwise(" "))
    corp_trend_df = merged.withColumn("metric_descriptor_code", when(mask, lit("Improve")).otherwise(col("metric_descriptor_code")))
    corp_trend_df = corp_trend_df.select("CORP_ID","IMPROVEMENT_IND_CODE","metric_descriptor_code","metric_descriptor_group_code","missing_reason_code")
    corp_trend_df=corp_trend_df.withColumn("SUBMISSION_FISCAL_YEAR", lit(str(closed_year-1)))
    corp_trend_final = corp_trend_df.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year"))).withColumnRenamed("CORP_ID", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("breakdown_type_code_l3", lit("N/A")).withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("segment_type_code", lit("N/A")).withColumn("segment_value_code", lit("N/A")).withColumn("reporting_type_code", lit("Pub"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0)).drop("SUBMISSION_FISCAL_YEAR").drop("IMPROVEMENT_IND_CODE")
    corp_trend_final=corp_trend_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    corp_trend_final=corp_trend_final.drop("reporting_entity_code")
    corp_trend_final = corp_trend_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "segment_type_code",
        "segment_value_code",
        "reporting_type_code",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    corp_trend_final = corp_trend_final.select(*desired_columns)
    
    # Show the new DataFrame to verify the column order
    #corp_trend_final.show()
    #corp_trend_final.printSchema()
    
    grouped_data_reg =  reg_trend.groupBy("adm_region_id").agg(
        collect_list(col("SUBMISSION_FISCAL_YEAR")).alias("times"),
        collect_list(col("PERCENTILE_90")).alias("percentiles")
    )
    los_reg_trend_a = grouped_data_reg.withColumn("regression_results", regression_udf(col("times"), col("percentiles")))
     
    # Expand the results into separate columns
    merged = los_reg_trend_a.select(
        "adm_region_id",
        col("regression_results").getItem(0).alias("slope"),
        col("regression_results").getItem(1).alias("intercept"),
        col("regression_results").getItem(2).alias("p_value"),
        col("regression_results").getItem(3).alias("std_err")
    )
     
    # Define improvement indicators
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", lit("002"))
    merged = merged.withColumn("metric_descriptor_code", lit("NoChange"))
    merged = merged.withColumn('missing_reason_code' , lit(None).cast(StringType()))
     
    # Applying the logic from the provided method
    mask = (merged['p_value'] < 0.05) & (merged['slope'] > 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("003")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_code", when(mask, lit("Weaken")).otherwise(col("metric_descriptor_code")))
     
    mask = (merged['p_value'] < 0.05) & (merged['slope'] < 0)
    merged = merged.withColumn("IMPROVEMENT_IND_CODE", when(mask, lit("001")).otherwise(col("IMPROVEMENT_IND_CODE")))
    merged = merged.withColumn("metric_descriptor_group_code", when(col("IMPROVEMENT_IND_CODE") .isNotNull(), "PerformanceTrend").otherwise(" "))
    
    reg_trend_df = merged.withColumn("metric_descriptor_code", when(mask, lit("Improve")).otherwise(col("metric_descriptor_code")))
    reg_trend_df = reg_trend_df.select("adm_region_id","IMPROVEMENT_IND_CODE","metric_descriptor_code","metric_descriptor_group_code","missing_reason_code")
    reg_trend_df=reg_trend_df.withColumn("SUBMISSION_FISCAL_YEAR", lit(str(closed_year-1)))
    reg_trend_final = reg_trend_df.withColumn("reporting_period_code", F.concat(F.lit("FY"), F.col("submission_fiscal_year"))).withColumnRenamed("adm_region_id", "reporting_entity_code")\
    .withColumn("reporting_entity_type_code", lit("ORG")).withColumn("indicator_code", lit("810"))\
    .withColumn("metric_code", lit("PCTL_90")).withColumn("breakdown_type_code_l1", lit("N/A"))\
    .withColumn("breakdown_value_code_l1", lit("N/A")).withColumn("breakdown_type_code_l2", lit("N/A")).withColumn("breakdown_value_code_l2", lit("N/A"))\
    .withColumn("breakdown_type_code_l3", lit("N/A")).withColumn("breakdown_value_code_l3", lit("N/A"))\
    .withColumn("segment_type_code", lit("N/A")).withColumn("segment_value_code", lit("N/A")).withColumn("reporting_type_code", lit("Pub"))\
    .withColumn("metric_result", lit(0)).withColumn("public_metric_result", lit(0)).drop("SUBMISSION_FISCAL_YEAR").drop("IMPROVEMENT_IND_CODE")
    
    reg_trend_final=reg_trend_final.withColumn("reporting_entity_code_text", col('reporting_entity_code').cast("string"))
    reg_trend_final=reg_trend_final.drop("reporting_entity_code")
    reg_trend_final = reg_trend_final.withColumnRenamed("reporting_entity_code_text", "reporting_entity_code")
    desired_columns = [
        "reporting_period_code",
        "reporting_entity_code",
        "reporting_entity_type_code",
        "indicator_code",
        "metric_code",
        "breakdown_type_code_l1",
        "breakdown_value_code_l1",
        "breakdown_type_code_l2",
        "breakdown_value_code_l2",
        "breakdown_type_code_l3",
        "breakdown_value_code_l3",
        "segment_type_code",
        "segment_value_code",
        "reporting_type_code",
        "metric_result",
        "metric_descriptor_group_code",
        "metric_descriptor_code",
        "missing_reason_code",
        "public_metric_result"
    ]
    
    # Create a new DataFrame with the columns in the desired sequence
    reg_trend_final = reg_trend_final.select(*desired_columns)
    
    # Show the new DataFrame to verify the column order
    #reg_trend_final.show()
    #reg_trend_final.printSchema()
    data_810 = final_set_df.unionByName(reg_trend_final).unionByName(corp_trend_final).unionByName(corp_comp_final)\
    .unionByName(reg_comp_final).orderBy('reporting_period_code','reporting_entity_code')
    data_submission_810 = data_810.filter(col("reporting_entity_code").isNotNull())


    # Stack all dataframes using reduce and unionAll()
    
    #reg_all = reduce(lambda x, y: x.unionAll(y), los_reg_all)
    #corp_all = reduce(lambda x, y: x.unionAll(y), los_corp_all)
    
    
    
    # Save intermediate or additional data for business area usage/reference (e.g.: for external data requests, DQ verification)
    intermediate_data_dict["data_submission_810" + str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = data_submission_810
    intermediate_data_dict["corp_all"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = corp_all
    intermediate_data_dict["reg_all"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = reg_all
    intermediate_data_dict["los_supp_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_corp
    intermediate_data_dict["los_supp_peer"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_peer
    intermediate_data_dict["los_supp_reg"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = los_supp_reg
    intermediate_data_dict["ps_all_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = ps_all_corp
    intermediate_data_dict["ucc_all_corp"+ str(closed_year-1) + "_" + datetime.now().strftime('%Y_%m_%d_%H_%M_%S')] = ucc_all_corp

    
    # Generate an output_df in a common format for your project
    # For indicator automation, output_df shares the same layout and requirements as shallow slice
    output_df = data_submission_810.select('*')
    
    # Return both output_df and dictionary of intermediate dataframes
    return output_df, intermediate_data_dict
