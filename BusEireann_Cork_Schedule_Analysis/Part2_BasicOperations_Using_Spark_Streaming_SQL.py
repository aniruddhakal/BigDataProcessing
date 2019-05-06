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

import pyspark
from pyspark.sql import functions
from pyspark.sql.functions import to_timestamp, to_date, month, udf, window
from datetime import datetime as DT
import pyspark.sql.types as types

import os
import shutil
import time


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval):
    # 1. We create the DataStreamWritter
    # 2. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True),
         ])

    # 3. Operation C1: We create the DataFrame from the dataset and the schema
    inputSDF = spark.readStream.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(monitoring_dir)

    # 4. Operation T1: We add the current timestamp
    my_frequency = str(time_step_interval) + " seconds"

    # 6. Operation T2: We add the watermark on my_time
    current_timestamp = pyspark.sql.functions.current_timestamp()
    inputSDF = inputSDF.withColumn("my_time", current_timestamp) \
        .withWatermark("my_time", "0 seconds")

    inputSDF.registerTempTable("tab")

    # group by window
    count = inputSDF.groupBy(window('my_time', my_frequency, my_frequency), inputSDF['my_time']) \
        .count()
    count = count.drop('my_time')
    count = count.drop('window')

    # 5. We set the frequency for the trigger
    # 7. We create the DataStreamWritter
    stream_writer = count.writeStream \
        .format("console") \
        .trigger(processingTime=my_frequency) \
        .outputMode("append")

    # stream_writer = count.writeStream \
    #     .format("csv") \
    #     .option("delimiter", ";") \
    #     .option("path", result_dir) \
    #     .option("checkpointLocation", checkpoint_dir) \
    #     .trigger(processingTime=my_frequency) \
    #     .outputMode("append")

    # 8. We return the DataStreamWritter
    return stream_writer


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, station_number):
    # 2. We define the Schema of our DF.
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

    # 3. Operation C1: We create the DataFrame from the dataset and the schema
    inputSDF = spark.readStream.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(monitoring_dir)

    # 5. We set the frequency for the trigger
    my_frequency = str(time_step_interval) + " seconds"

    # 4. Operation T1: We add the current timestamp
    inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.current_timestamp()) \
        .withWatermark("my_time", "0 seconds")
    inputSDF.registerTempTable("tab")

    # 6. Operation T2: We filter the items that belong to station_number.
    station_data = spark.sql("select my_time, date from tab where station_number = '" + str(station_number) + "'")
    station_data.registerTempTable("tab")

    # 7. Operation T3: We get the info we are interested into
    # 8. Operation T4: We count the entries per date
    count = spark.sql("select my_time, date, count(date) as count from tab group by date, my_time")
    count = count.drop("my_time")

    # stream_writer = count.writeStream \
    #     .format("console") \
    #     .trigger(processingTime=my_frequency) \
    #     .outputMode("complete")

    # 9. We create the DataStreamWritter
    stream_writer = count.writeStream \
        .format("csv") \
        .option("delimiter", ";") \
        .option("path", result_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=my_frequency) \
        .outputMode("append")

    # 10. We return the DataStreamWritter
    return stream_writer


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, station_number):
    # 2. We define the Schema of our DF.
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

    # 3. Operation C1: We create the DataFrame from the dataset and the schema
    inputSDF = spark.readStream.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(monitoring_dir)

    my_frequency = str(time_step_interval) + " seconds"

    # 4. Operation T1: We add the current timestamp
    inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.current_timestamp()) \
        .withWatermark("my_time", "0 seconds")

    inputSDF.registerTempTable("tab")

    # 6. Operation T2: We filter the items that belong to station_number.
    station_data = spark.sql("select * from tab where station_number = '" + str(station_number) + "'")
    station_data.registerTempTable("tab")

    # 7. Operation T3: We get the info we are interested into
    transform_columns_dates = spark.sql(
        "select my_time, to_timestamp(scheduled_time, 'HH:mm:ss') as schedule, to_timestamp(expected_arrival_time, 'HH:mm:ss') as arrival from tab")

    transform_columns_dates.registerTempTable("tab")

    # renaming column just for the sake of shortening lines
    tt = transform_columns_dates

    # 8. Operation T4: We compute whether the bus is ahead or behind schedule
    # 9. Operation T5: We group the data on whether it is ahead or behind schedule
    behind = tt.where(tt['schedule'] < tt['arrival']).groupBy(window("my_time", my_frequency, my_frequency),
                                                              tt['my_time']).count()
    ahead = tt.where(tt['schedule'] >= tt['arrival']).groupBy(window("my_time", my_frequency, my_frequency),
                                                              tt['my_time']).count()

    behind = behind.drop('my_time')
    behind = behind.drop('window')

    ahead = ahead.drop('my_time')
    ahead = ahead.drop('window')
    #
    get_ahead = udf(lambda x: 'Ahead')
    get_behind = udf(lambda x: 'Behind')

    behind = behind.withColumn('Behind', get_behind(behind['count']))
    ahead = ahead.withColumn('Ahead', get_ahead(ahead['count']))

    # 10. We create the DataStreamWritter
    # 5. We set the frequency for the trigger
    behind.writeStream \
        .format("console") \
        .trigger(processingTime=my_frequency) \
        .outputMode("append") \
        .start()

    stream_writer = ahead.writeStream \
        .format("console") \
        .trigger(processingTime=my_frequency) \
        .outputMode("append")


    # 11. We return the DataStreamWritter
    return stream_writer


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, station_number):
    # 2. We define the Schema of our DF.
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

    # 3. Operation C1: We create the DataFrame from the dataset and the schema
    inputSDF = spark.readStream.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(monitoring_dir)

    # 4. Operation T1: We add the current timestamp
    inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.current_timestamp()) \
        .withWatermark("my_time", "0 seconds")

    inputSDF.registerTempTable("tab")

    # 6. Operation T2: We filter the items that belong to station_number.
    # 7. Operation T3: We get the info we are interested into
    # 8. Operation T2: We count the entries per day of the week and scheduled time
    station_data = spark.sql(
        "select my_time, day_of_week, scheduled_time, count(scheduled_time) as count from tab where station_number = '" + str(
            station_number) + "' group by day_of_week, scheduled_time, my_time")

    station_data = station_data.drop('my_time')

    # 5. We set the frequency for the trigger
    my_frequency = str(time_step_interval) + " seconds"

    # stream_writer = station_data.writeStream \
    #     .format("console") \
    #     .trigger(processingTime=my_frequency) \
    #     .outputMode("append")

    # 9. We create the DataStreamWritter
    stream_writer = station_data.writeStream \
        .format("csv") \
        .option("delimiter", ";") \
        .option("path", result_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=my_frequency) \
        .outputMode("append")

    # 10. We return the DataStreamWritter
    return stream_writer


# ------------------------------------------
# FUNCTION get_waiting_time
# ------------------------------------------
def get_waiting_time(my_time1, my_time2):
    pass


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, station_number, month_list):
    # 2. We define the Schema of our DF.
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

    # 3. Operation C1: We create the DataFrame from the dataset and the schema
    inputSDF = spark.readStream.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(monitoring_dir)

    # 4. Operation T1: We add the current timestamp
    current_timestamp = pyspark.sql.functions.current_timestamp()
    inputSDF = inputSDF.withColumn("my_time", current_timestamp) \
        .withWatermark("my_time", "0 seconds")

    inputSDF.registerTempTable("tab")

    # 6. Operation T2: We filter the items that belong to station_number.
    station_data = spark.sql(
        "select * from tab where station_number = '" + str(
            station_number) + "'")
    station_data.registerTempTable("tab")

    # transform columns day, scheduled_time, expected_arrival_time to respective data_types
    transformed_df = spark.sql(
        "select my_time, day_of_week as day, date, to_date(date, 'dd/MM/yyyy') as new_date, to_timestamp(query_time, 'HH:mm:ss') as query,"
        " to_timestamp(expected_arrival_time, 'HH:mm:ss') as arrival from tab")

    transformed_df.registerTempTable("tab")

    # only select rows where bus was late than scheduled during semester 1 months.
    transformed_df = spark.sql(
        "select my_time, day, date, query, arrival from tab where month(new_date) >= 9 and month(new_date) < 12")
    transformed_df.registerTempTable("tab")


    # 7. Operation T3: We get the entries for the right months
    # 10.1. We define the UDF function we will use
    get_day_of_month = udf(lambda x, y: x + " " + y.split("/")[1])
    day_of_month = transformed_df.withColumn('day_of_month', get_day_of_month(transformed_df['day'],
                                                                              transformed_df['date']))
    get_waiting_time = udf(lambda x, y: (y - x).total_seconds())

    # 10.2. We apply the UDF
    # 10. Operation T6: We compute the waiting time
    waiting_time = day_of_month.withColumn('waiting_time',
                                           get_waiting_time(day_of_month['query'], day_of_month['arrival']))
    waiting_time.registerTempTable("tab")


    # 8. Operation T4: We project the info we are interested into
    # 9. Operation T5: We put together the day of the week and the month
    # 11. Operation T7: We compute the average waiting time
    result = spark.sql(
        "select my_time, day_of_month as new_date, avg(waiting_time) as avg_time from tab group by day_of_month, my_time")
    # result = spark.sql(
    #     "select my_time, day_of_month as new_date, avg(waiting_time) as avg_time from tab group by day_of_month, my_time order by avg_time")

    # 5. We set the frequency for the trigger
    my_frequency = str(time_step_interval) + " seconds"

    result = result.drop('my_time')

    # stream_writer = result.writeStream \
    #     .format("console") \
    #     .trigger(processingTime=my_frequency) \
    #     .outputMode("complete")

    # 12. We create the DataStreamWritter
    stream_writer = result.writeStream \
            .format("csv") \
        .option("delimiter", ";") \
        .option("path", result_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=my_frequency) \
        .outputMode("append")

    # 13. We return the DataStreamWritter
    return stream_writer


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(local_False_databricks_True, source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = []
    if local_False_databricks_True == False:
        fileInfo_objects = os.listdir(source_dir)
    else:
        fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)

        # 3.2. If the file is processed in DBFS
        if local_False_databricks_True == True:
            # 3.2.1. We look for the pattern name= to remove all useless info from the start
            lb_index = file_name.index("name='")
            file_name = file_name[(lb_index + 6):]

            # 3.2.2. We look for the pattern ') to remove all useless info from the end
            ub_index = file_name.index("',")
            file_name = file_name[:ub_index]

        # 3.3. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We sort the list in alphabetic order
    res.sort()

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(local_False_databricks_True, source_dir, verbose)

    # 2. We get the starting time of the process
    time.sleep(time_step_interval * 0.1)

    start = time.time()

    # 2.1. If verbose mode, we inform of the starting time
    if (verbose == True):
        print("Start time = " + str(start))

    # 3. We set a counter in the amount of files being transferred
    count = 0

    # 4. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 4.1. We copy the file from source_dir to dataset_dir#
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file)
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file)

        # 4.2. We increase the counter, as we have transferred a new file
        count = count + 1

        # 4.3. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

        # 4.3. We rename

        # 4.4. We wait the desired transfer_interval until next time slot.
        time.sleep((start + (count * time_step_interval)) - time.time())

    # 5. We wait a last time_step_interval
    time.sleep(time_step_interval)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            time_step_interval,
            verbose,
            option
            ):
    # 1. We get the DataStreamWriter object derived from the model
    dsw = None

    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        dsw = ex1(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.

    # Note: Cannot be done as Structured Streaming does not support distinct operations
    # Thus, we collect: The date of each day we have collected data for.
    #                   For each of these dates, the amount of measurements we have found for it.
    # Eg: One of the entries computed by the file is:
    # 02/10/2016;5
    # Meaning that we found 5 measurement entries for day 02/10/2016 and station 240101.
    if option == 2:
        dsw = ex2(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        dsw = ex3(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.

    # Note: Cannot be done as Structured Streaming does not support distinct operations
    # Thus, we collect: The date of each day we have collected data for.
    #                   For each of these dates, the amount of measurements we have found for it.
    # Eg: One of the entries computed by the file is:
    # Tuesday;16:11:00;96
    # Meaning that we found 96 measurement entries for Tuesday and bus_scheduled at 16:11:00 at station 241111
    if option == 4:
        dsw = ex4(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.

    # Note: Cannot sort as sorting is only supported after an aggregation and Complete mode (not append mode).
    if option == 5:
        dsw = ex5(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval, 240491, ['09', '10', '11'])

    # 2. We get the StreamingQuery object derived from starting the DataStreamWriter
    ssq = dsw.start()

    # 3. We stop the StreamingQuery to finish the application
    ssq.awaitTermination(time_step_interval)

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir
    streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the StreamingQuery
    ssq.stop()


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 3

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    # my_databricks_path = "/"
    #
    # source_dir = "FileStore/tables/7_Assignments/A02/my_dataset_complete/"
    # monitoring_dir = "FileStore/tables/7_Assignments/A02/my_monitoring/"
    # checkpoint_dir = "FileStore/tables/7_Assignments/A02/my_checkpoint/"
    # result_dir = "FileStore/tables/7_Assignments/A02/my_result/"

    my_local_path = ""
    my_databricks_path = ""

    source_dir = "../my_dataset_complete/"
    monitoring_dir = "../streaming_dirs/monitoring/"
    checkpoint_dir = "../streaming_dirs/checkpoint/"
    result_dir = "../streaming_dirs/result/"

    if local_False_databricks_True == False:
        source_dir = my_local_path + source_dir
        monitoring_dir = my_local_path + monitoring_dir
        checkpoint_dir = my_local_path + checkpoint_dir
        result_dir = my_local_path + result_dir
    else:
        source_dir = my_databricks_path + source_dir
        monitoring_dir = my_databricks_path + monitoring_dir
        checkpoint_dir = my_databricks_path + checkpoint_dir
        result_dir = my_databricks_path + result_dir

    # 4. We set the Spark Streaming parameters

    # 4.1. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 7

    # 4.2. We configure verbosity during the program run
    verbose = False

    # 5. We remove the directories
    if local_False_databricks_True == False:
        # 5.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)

        # 5.2. We remove the result_dir
        if os.path.exists(result_dir):
            shutil.rmtree(result_dir)

        # 5.3. We remove the checkpoint_dir
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
    else:
        # 5.1. We remove the monitoring_dir
        dbutils.fs.rm(monitoring_dir, True)

        # 5.2. We remove the result_dir
        dbutils.fs.rm(result_dir, True)

        # 5.3. We remove the checkpoint_dir
        dbutils.fs.rm(checkpoint_dir, True)

    # 6. We re-create the directories again
    if local_False_databricks_True == False:
        # 6.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)

        # 6.2. We re-create the result_dir
        os.mkdir(result_dir)

        # 6.3. We re-create the checkpoint_dir
        os.mkdir(checkpoint_dir)
    else:
        # 6.1. We re-create the monitoring_dir
        dbutils.fs.mkdirs(monitoring_dir)

        # 6.2. We re-create the result_dir
        dbutils.fs.mkdirs(result_dir)

        # 6.3. We re-create the checkpoint_dir
        dbutils.fs.mkdirs(checkpoint_dir)

    # 7. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 8. We call to our main function
    my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            time_step_interval,
            verbose,
            option
            )
