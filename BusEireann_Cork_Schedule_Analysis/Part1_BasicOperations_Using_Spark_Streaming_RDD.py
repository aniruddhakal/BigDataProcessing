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
import pyspark.streaming
from _datetime import datetime

import os
import shutil
import time


# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line_dates(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res

    # ----------
    # Schema:
    # ----------
    # 0 -> station_number
    # 1 -> station_name
    # 2 -> direction
    # 3 -> day_of_week
    # 4 -> date
    # 5 -> query_time
    # 6 -> scheduled_time
    # 7 -> expected_arrival_time

    if (len(params) == 8):
        res = (int(params[0]),
               str(params[1]),
               str(params[2]),
               str(params[3]),
               datetime.strptime(params[4], "%d/%m/%Y"),
               datetime.strptime(params[5], "%H:%M:%S"),
               datetime.strptime(params[6], "%H:%M:%S"),  # 07:10:00
               datetime.strptime(params[7], "%H:%M:%S")  # 07:10:00
               )

    # 5. We return res
    return res


def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res

    # ----------
    # Schema:
    # ----------
    # 0 -> station_number
    # 1 -> station_name
    # 2 -> direction
    # 3 -> day_of_week
    # 4 -> date
    # 5 -> query_time
    # 6 -> scheduled_time
    # 7 -> expected_arrival_time

    if (len(params) == 8):
        res = (int(params[0]),
               str(params[1]),
               str(params[2]),
               str(params[3]),
               str(params[4]),
               str(params[5]),
               str(params[6]),
               str(params[7])
               )

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(ssc, monitoring_dir):
    ssc.textFileStream(monitoring_dir).count().pprint()


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(ssc, monitoring_dir, station_number):
    rdd = ssc.textFileStream(monitoring_dir).cache()
    key_val = rdd.map(lambda x: process_line(x)).filter(lambda x: x[0] == station_number).map(lambda x: (x[4], x))
    count = key_val.groupByKey().count()
    count.pprint()


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(ssc, monitoring_dir, station_number):
    # 1. Operation C1: We read the dataset batches from monitoring_dir
    rdd = ssc.textFileStream(monitoring_dir).cache()

    # 2. Operation T1: We process each line to get the relevant info
    # 3. Operation T2: We filter the items that belong to station_number.
    rows = rdd.map(lambda x: process_line(x)).filter(lambda x: x[0] == station_number).cache()

    # 4. Operation T3: We get the info we are interested into
    # 5. Operation T4: We compute whether the bus is ahead or behind schedule
    # 6. Operation T5: We compute the total amount of buses ahead and behind
    behind = rows.filter(lambda x: x[6] < x[7]).count()
    ahead = rows.filter(lambda x: x[6] >= x[7]).count()

    # 7. Operation O1: We print by console the result of each batch
    ahead.pprint()
    behind.pprint()


# ------------------------------------------
# FUNCTION my_sort
# ------------------------------------------
def my_sort(my_list):
    pass

    # 1. We create the variable to return

    # 2. We sort the list

    # 3. We return res


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(ssc, monitoring_dir, station_number):
    rdd = ssc.textFileStream(monitoring_dir).cache()

    cit_schedules = rdd.map(lambda x: process_line(x)).filter(lambda x: x[0] == station_number)

    # this is one valid way - as per my stackoverflow answer:
    # https://stackoverflow.com/a/54032126/3149277
    # key_val_pairs = cit_schedules.map(lambda x: (x[3], x[6])).groupByKey().map(lambda x: (x[0], sorted(set(x[1]))))

    # another alternative way - as per my stackoverflow answer:
    # https://stackoverflow.com/a/54032126/3149277
    key_val_pairs = cit_schedules.map(lambda x: (x[3], x[6])).groupByKey().mapValues(set).mapValues(sorted)

    key_val_pairs.pprint()


# ------------------------------------------
# FUNCTION time_to_int
# ------------------------------------------
def time_to_int(my_time):
    pass


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(ssc, monitoring_dir, station_number, month_list):
    # ssc = pyspark.streaming.StreamingContext(ssc, 1)
    rdd = ssc.textFileStream(monitoring_dir).cache()
    rdd = rdd.map(lambda x: process_line_dates(x))

    # get all rows for semester1 months for this station_number
    semester1_months_waiting = rdd.filter(lambda x: x[4].month >= 9 and x[4].month < 12 and x[0] == station_number)

    def get_waiting_time(x, y):
        return (y - x).total_seconds()

    waiting_time = semester1_months_waiting.map(lambda x: (x, get_waiting_time(x[5], x[7])))

    def get_day_and_month_string(x, y):
        return x + " " + ("%02d" % y.month)

    day_of_month = waiting_time.map(lambda x: (get_day_and_month_string(x[0][3], x[0][4]), x[1])) \
        .combineByKey(lambda x: (x, 1),
                      lambda x, y: (x[0] + y, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))

    day_of_month = day_of_month.transform(lambda rdd: rdd.sortBy(lambda x: x[1], True))

    day_of_month.pprint(100)


# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(ssc, monitoring_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(ssc, monitoring_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(ssc, monitoring_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(ssc, monitoring_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(ssc, monitoring_dir, 240491, ['09', '10', '11'])


# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(sc, monitoring_dir, result_dir, max_micro_batches, time_step_interval, option):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = pyspark.streaming.StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, option)

    # 4. We return the ssc configured and modelled.
    return ssc


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

            # 4.4. We wait the desired transfer_interval until next time slot.
        time.sleep((start + (count * time_step_interval)) - time.time())


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            option
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = pyspark.streaming.StreamingContext.getActiveOrCreate(checkpoint_dir,
                                                               lambda: create_ssc(sc,
                                                                                  monitoring_dir,
                                                                                  result_dir,
                                                                                  max_micro_batches,
                                                                                  time_step_interval,
                                                                                  option
                                                                                  )
                                                               )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 6. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 5

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    # my_databricks_path = "/"
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

    # 4.1. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 12

    # 4.2. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 2

    # 4.3. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 4.4. We configure verbosity during the program run
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

    # 7. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 8. We call to our main function
    my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            option
            )
