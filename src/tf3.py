from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.master("local[*]") \
    .appName("Minipro") \
    .config("spark.jars", "/var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar") \
    .enableHiveSupport().getOrCreate()

# Ensure the Hive database exists
spark.sql("CREATE DATABASE IF NOT EXISTS lokhandwala1")

# Read data from PostgreSQL
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://18.170.23.150:5432/testdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "demo_hasan") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .load()

# Print Schema
df.printSchema()

# Save Data to Hive
df.write.mode("overwrite").saveAsTable("lokhandwala1.demohas")
print("Successfully Loaded to Hive")
