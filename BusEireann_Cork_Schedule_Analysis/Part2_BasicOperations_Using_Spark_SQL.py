# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

from time import time

import pyspark
from pyspark.sql import functions
from pyspark.sql.functions import udf
from pyspark.sql.functions import to_timestamp, to_date, month


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
         ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    print(inputDF.count())


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(spark, my_dataset_dir, station_number):
    # spark = pyspark.sql.session.SparkSession()
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
         ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    inputDF.persist()

    inputDF.registerTempTable("tab")

    station_data = spark.sql("select date from tab where station_number = '" + str(station_number) + "'")
    station_data.registerTempTable("tab")
    count = spark.sql("select count(distinct(date)) as count from tab")

    result = count.collect()[0]
    print(result['count'])


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(spark, my_dataset_dir, station_number):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
         ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    inputDF.registerTempTable("tab")
    station_data = spark.sql("select * from tab where station_number = '" + str(station_number) + "'")
    station_data.registerTempTable("tab")
    transform_columns_dates = spark.sql(
        "select to_timestamp(scheduled_time, 'HH:mm:ss') as schedule, to_timestamp(expected_arrival_time, 'HH:mm:ss') as arrival from tab")

    transform_columns_dates.registerTempTable("tab")
    behind = spark.sql("select count(schedule) as behind from tab where schedule < arrival")
    ahead = spark.sql("select count(schedule) as ahead from tab where schedule >= arrival")

    the_ahead = ahead.collect()[0]['ahead']
    get_behind = udf(lambda x: the_ahead)

    behind = behind.withColumn('ahead', get_behind(behind['behind']))

    print(behind.collect())


# ------------------------------------------
# FUNCTION my_sort
# ------------------------------------------
def my_sort(my_list):
    pass


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(spark, my_dataset_dir, station_number):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
         ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    station_data = inputDF.filter(inputDF["station_number"] == str(station_number)) \
        .select(inputDF['day_of_week'], inputDF['scheduled_time']) \
        .distinct() \
        .orderBy(inputDF['scheduled_time'])

    result = station_data.groupBy('day_of_week') \
        .agg(functions.collect_list('scheduled_time')
             .alias('bus_scheduled_times'))

    for res in result.collect():
        print(res)


# ------------------------------------------
# FUNCTION get_waiting_time
# ------------------------------------------
def get_waiting_time(my_time1, my_time2):
    pass


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(spark, my_dataset_dir, station_number, month_list):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
         ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    inputDF.registerTempTable("tab")

    # select only desired stations
    station_data = spark.sql(
        "select * from tab where station_number = '" + str(
            station_number) + "'").cache()
    station_data.registerTempTable("tab")

    # transform columns day, scheduled_time, expected_arrival_time to respective data_types
    transformed_df = spark.sql(
        "select day_of_week as day, date, to_date(date, 'dd/MM/yyyy') as new_date, to_timestamp(query_time, 'HH:mm:ss') as query,"
        " to_timestamp(expected_arrival_time, 'HH:mm:ss') as arrival from tab").cache()

    transformed_df.registerTempTable("tab")

    # only select rows where bus was late than scheduled during semester 1 months.
    transformed_df = spark.sql(
        "select day, date, query, arrival from tab where month(new_date) >= 9 and month(new_date) < 12").cache()
    transformed_df.registerTempTable("tab")

    get_day_of_month = udf(lambda x, y: x + " " + y.split("/")[1])
    day_of_month = transformed_df.withColumn('day_of_month', get_day_of_month(transformed_df['day'],
                                                                              transformed_df['date']))

    get_waiting_time = udf(lambda x, y: (y - x).total_seconds())
    waiting_time = day_of_month.withColumn('waiting_time',
                                           get_waiting_time(day_of_month['query'], day_of_month['arrival']))
    waiting_time.registerTempTable("tab")
    result = spark.sql(
        "select day_of_month as new_date, avg(waiting_time) as avg_time from tab group by day_of_month order by avg_time").collect()

    for r in result:
        print(r)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(spark, my_dataset_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(spark, my_dataset_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(spark, my_dataset_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(spark, my_dataset_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(spark, my_dataset_dir, 240491, ['09', '10', '11'])


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 1

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    # my_databricks_path = "/"
    my_local_path = ""
    my_databricks_path = ""

    # my_dataset_dir = "FileStore/tables/7_Assignments/A02/my_dataset_single_file/"
    my_dataset_dir = "../my_dataset_single_file"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    start = time()
    # 5. We call to our main function
    my_main(spark, my_dataset_dir, option)
    finish = time() - start
    print("\n\nTime taken %f seconds" % finish)
