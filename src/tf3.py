from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Postgres to Hive") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.jars", "file:///C:/Users/44754/Downloads/postgresql-42.5.3.jar") \
    .config("spark.sql.hive.metastore.uris", "thrift://18.169.244.191:8889") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.extraLibraryPath", "C:/hadoop/bin") \
    .config("spark.executor.extraLibraryPath", "C:/hadoop/bin") \
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
    .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://172.31.3.80:8020")

# Read data from PostgreSQL
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "tfl_underground_pyspark") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022").load()

df.printSchema()

# Convert 'Timestamp' to proper timestamp format and replace "N/A" with null
df_transformed = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))
df_transformed = df_transformed.replace("N/A", None)

# Show DataFrame schema and data preview
df_transformed.printSchema()
df_transformed.show()

# Write data to Hive
try:
    df_transformed.write.mode("append").saveAsTable("default.tfl_data2")
    print("Successfully Loaded to Hive")
except Exception as e:
    print(f"An error occurred: {e}")

# Stop the SparkSession
spark.stop()
