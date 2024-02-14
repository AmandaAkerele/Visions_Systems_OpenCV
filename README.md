spark = SparkSession.builder \
    .appName("example") \
    .config("spark.driver.memory", "4g") \  # Adjust the memory size as per your requirement
    .getOrCreate()
