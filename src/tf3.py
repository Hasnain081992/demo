from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local(4)").appName("Miniproject").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.170.23.150:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "tfl_underground_pyspark").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

df.write.mode("overwrite").saveAsTable("lokhandwala.tfl_underground_pyspark")
print("Successfully Load to Hive")

# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/full_load_postgresToHive.py