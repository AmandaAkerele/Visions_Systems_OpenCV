# create spark session
spark_token = "D1tlsoCjLWJqJi6yt78dmbSv8WxgEcFop0me4sSJb7I="

#### initialize spark session
extraClassPath = None
for file in os.listdir("/opt/jars"):
    # check only text files
    if file.endswith('.jar'):
        if extraClassPath is None:
            extraClassPath = f"/opt/jars/{file}"
        else:
            extraClassPath += f",/opt/jars/{file}"
            
# connect to spark cluster
spark = (SparkSession.builder
                     .master(f"spark://spkm-aakerele:7077")
                     .appName("OracleSparkApp")
                     .config("spark.authenticate", "true")
                     .config("spark.authenticate.secret", spark_token)
                     .config("spark.driver.memory", "64g")
                     .config("spark.executor.memory", "64g")
                     .config('spark.executor.cores', '4')
                     .config("spark.jars", extraClassPath)
                     .config("spark.driver.extraClassPath", extraClassPath)
                     .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
                     .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                     .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                     .config("spark.sql.debug.maxToStringFields", 1000)
                     .config('spark.sql.timestampType', "TIMESTAMP_NTZ")
                     .getOrCreate())

