from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Create a SparkSession in local mode
spark = (SparkSession.builder
    
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-common:3.2.1,org.apache.hadoop:hadoop-hdfs:3.2.1")
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    .appName("HDFS Data Reader (Local)")
    .getOrCreate())


# Define the HDFS path to your data file
hdfs_path1 = "hdfs://namenode:9000/raw_zone/fact/activity/31a5298d-c2ba-4764-b8d9-91a5baa182bd"  
hdfs_path3 = "hdfs://namenode:9000/raw_zone/fact/activity/danh_sach_sv_de.csv"

# Read the data from HDFS as a DataFrame based on file format
data_df1 = spark.read.format("parquet").load(hdfs_path1)
data_df3 = spark.read.csv(hdfs_path3,header=None)


column_renames2 = [("_c0", "student_code"), ("_c1", "student_name")]

for old_name, new_name in column_renames2:
  data_df3 = data_df3.withColumnRenamed(old_name, new_name)


# joined_df = data_df1.unionByName(data_df2, allowMissingColumns=True)
joined_df1 = data_df1.join(data_df3, on="student_code", how="inner")
joined_df1 = joined_df1.withColumn("date", to_date(col("timestamp"),format= "M/d/yyyy"))


# Extract year, month, and day
joined_df1 = joined_df1.withColumn("year", year(col("date")).cast("string"))
joined_df1 = joined_df1.withColumn("month", month(col("date")).cast("string"))
joined_df1 = joined_df1.withColumn("day", dayofmonth(col("date")).cast("string"))

# Format date as YYYYMMDD
joined_df1 = joined_df1.withColumn("date1", concat(col("year") ,lpad(col("month"),2,"0"), lpad( col("day"),2,"0")))


result_df = joined_df1.groupBy("date", "student_code", "student_name", "activity","date1","year","month","day").agg(sum("numberOfFile").cast("int").alias("TotalFile"))

result1_df = result_df.select(col("date1").alias("date"),col("student_code"), col("student_name"), col("activity"),col("TotalFile").alias("totalFile"))

# Sort by date, student code, and activity
result1_df = result1_df.orderBy("date", "student_code", "activity")

result1_df.write.csv("hdfs://namenode:9000/raw_zone/fact/activity/result",header=True)
spark.stop()
