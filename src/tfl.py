from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Postgres to Hive") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.jars", "file:///C:/Users/44754/Downloads/postgresql-42.5.3.jar") \
    .config("spark.sql.hive.metastore.uris", "thrift://18.169.244.191:8889") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://172.31.3.80:8020")

# Show available databases
spark.sql("SHOW DATABASES").show()
spark.sql("USE default")
spark.sql("SHOW TABLES").show()

# PostgreSQL connection properties
url = "jdbc:postgresql://18.170.23.150/testdb?ssl=false"  # Replace with your PostgreSQL connection details
properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Step 1: Read data from PostgreSQL into a Spark DataFrame
try:
    df = spark.read.format("jdbc").options(
        url=url,
        dbtable="tfl_underground_pyspark",
        user=properties["user"],
        password=properties["password"],
        driver=properties["driver"]
    ).load()
    
    print("Data successfully read from PostgreSQL:")
    df.show(50)

    # Step 2: Transform - Clean and Format the Data
    # Convert 'Timestamp' to proper timestamp format
    df_transformed = df.withColumn("Timestamp", F.to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))

    # Replace "N/A" with null
    df_transformed = df_transformed.replace("N/A", None)
    
    # Step 3: Insert Transformed Data into Hive Table
    df_transformed.write.mode("overwrite").saveAsTable("default.tfl_data")  # Replace "your_hive_table" with the desired Hive table name
    
    print("Data successfully pushed to Hive table.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the SparkSession
    spark.stop()
