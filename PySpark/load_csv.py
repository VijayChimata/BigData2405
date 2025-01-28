from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, trim, date_format, to_timestamp, to_date, hour, countDistinct, unix_timestamp, lag, max, min



spark = SparkSession.builder \
    .appName("CSV Data Loading") \
    .getOrCreate()

# Load CSV file into a DataFrame
df = spark.read.csv("./data/web_visits2.txt", header=True, inferSchema=True)

# Show the first 5 rows
df.show(5)


##3. Remove leading and Trailing spaces from each field in the Dataframe and convert the timestamp to Month/Day/Year Hour:Minute:seconds format

# Remove leading and trailing spaces
df_cleaned = df.select([trim(col(c)).alias(c) for c in df.columns])

# Convert timestamp column to the required format
#df_cleaned = df_cleaned.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
df_cleaned = df_cleaned.withColumn("timestamp", date_format(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))

df_cleaned.show(5)


## 4.Create a new field called Sys_date. The value of this field for each row should be the Date extracted from the column Timestamp in the Dataframe

# Convert the string to a timestamp first, then extract the date part
df_cleaned = df_cleaned.withColumn("timestamp", F.to_timestamp(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))

# Now, extract the date from the timestamp
df_cleaned = df_cleaned.withColumn("Sys_date", F.to_date(col("timestamp")))

df_cleaned.show(5)
df_cleaned.printSchema()


## 5. Write the output to a Parquet file and store it in HDFS.
#df_cleaned.write.parquet("./data/parquet_output")


## 6. Create another Dataframe and load the data from the Parquet file in step 5

df_parquet = spark.read.parquet("./data/parquet_output")
df_parquet.show(5)


## Find out the below insights by either running sql or dataframe functions on the Dataframe created in step6.
## a. Find out the total number of visits per each title
df_parquet.groupBy("title").count().show()

## b. Find out the Hour of the Day with most visits overall
df_parquet.withColumn("hour_of_day", hour(col("timestamp"))).groupBy("hour_of_day").count().orderBy("count", ascending=False).show(1)

## c. Find out the User with most visits
df_parquet.groupBy("name").count().orderBy("count", ascending=False).show(1)

## d. Find out the User with most visits for 'Remote Support: Geek Squad - Best Buy'
df_parquet.filter(df_parquet.title == "Remote Support: Geek Squad - Best Buy") \
    .groupBy("name").count().orderBy("count", ascending=False).show(1)

## e. Find out the number of users who has both 'Best Buy Support & Customer Service' and 'Remote Support: Geek Squad - Best Buy'
df_both = df_parquet.filter(df_parquet.title.isin("Best Buy Support & Customer Service", "Remote Support: Geek Squad - Best Buy")) \
    .groupBy("name").agg(countDistinct("title").alias("title_count")) \
    .filter(col("title_count") == 2).count()

print(f"Number of users with both titles: {df_both}")

## f. Find out the number of users who has both 'Best Buy Support & Customer Service' and 'Schedule a Service - Best Buy'
df_both_schedule_service = df_parquet.filter(df_parquet.title.isin("Best Buy Support & Customer Service", "Schedule a Service - Best Buy")) \
    .groupBy("name").agg(countDistinct("title").alias("title_count")) \
    .filter(col("title_count") == 2).count()

print(f"Number of users with both titles: {df_both_schedule_service}")


## g. Find the User who has the longest time interval between visits.
# Sort the data by user and timestamp
df_sorted = df_parquet.withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .orderBy("name", "timestamp")

# Calculate the time difference between consecutive visits for each user
df_sorted = df_sorted.withColumn("prev_timestamp", lag("timestamp").over(Window.partitionBy("name").orderBy("timestamp")))
df_sorted = df_sorted.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))

# Find the user with the longest time difference
df_sorted.groupBy("name").agg(max("time_diff").alias("max_time_diff")).orderBy("max_time_diff", ascending=False).show(1)

## h. Find the User with the shortest time interval between visits.
df_sorted.groupBy("name").agg(min("time_diff").alias("min_time_diff")).orderBy("min_time_diff").show(1)
