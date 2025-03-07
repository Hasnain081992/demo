from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, to_date
from pyspark.sql.types import DateType

PG_TABLE_NAME= "tfl_underground_pyspark"
HIVE_TABLE_NAME="tfl_data2"

def initialize_spark():
    """Initialize the Spark session."""
    return SparkSession.builder.master("local").appName("tfl").enableHiveSupport().getOrCreate()

def load_data_from_postgres(spark):
    """Load data from PostgreSQL."""
    return spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", PG_TABLE_NAME).option("user", "consultants").option("password", "WelcomeItc@2022").load()
    
def transform_data(df):
    """Apply transformations to the dataframe."""
    # Transformation 1: Fill empty 'category' with 'travel'
    df_transformed = df.withColumn("Timestamp", F.to_timestamp(col("Timestamp"), "dd/MM/yyyy HH:mm"))
    
    # Transformation 2: Drop 'cc_num' column
    df_transformed = df.replace("N/A", None)
    
    

    
    
    return df

def save_to_hive(df):
    """Write the transformed dataframe to Hive."""
    df.write.mode("overwrite").saveAsTable("default.{}".format(HIVE_TABLE_NAME))
    print("Successfully Loaded to Hive")


if __name__ == "__main__":
    # Initialize Spark
    spark = initialize_spark()
    
    # Load data from PostgreSQL
    raw_df = load_data_from_postgres(spark)
    
    # Transform the data
    transformed_df = transform_data(raw_df)
    
    # Save the transformed data to Hive
    save_to_hive(transformed_df)

#cd Pyspark
#spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar sop_loadToHive.py