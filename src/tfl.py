from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def initialize_spark():
    """Initialize the Spark session."""
    return (
        SparkSession.builder.master("local")
        .appName("Minidemo")
        .enableHiveSupport()
        .getOrCreate()
    )

def load_data_from_postgres(spark):
    """Load data from PostgreSQL."""
    return (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://18.170.23.150:5432/testdb")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "tfl_underground_pyspark")
        .option("user", "consultants")
        .option("password", "WelcomeItc@2022")
        .load()
    )

def transform_data(df):
    """Apply transformations to the DataFrame."""
    return (
        df.withColumn("Timestamp", F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm"))
        .na.replace("N/A", None)  # Replacing "N/A" with NULL
    )

def save_to_hive(df):
    """Write the transformed DataFrame to Hive."""
    df.write.mode("overwrite").saveAsTable("default.tfl_data")
    print("Successfully Loaded to Hive")

if __name__ == "__main__":
    spark = initialize_spark()
    raw_df = load_data_from_postgres(spark)
    transformed_df = transform_data(raw_df)
    save_to_hive(transformed_df)
