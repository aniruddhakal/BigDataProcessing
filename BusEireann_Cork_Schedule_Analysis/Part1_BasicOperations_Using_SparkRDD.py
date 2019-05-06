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
from time import time
from datetime import datetime


# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
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
               datetime.strptime(params[4], "%d/%m/%Y"),
               datetime.strptime(params[5], "%H:%M:%S"),
               datetime.strptime(params[6], "%H:%M:%S"),  # 07:10:00
               datetime.strptime(params[7], "%H:%M:%S")  # 07:10:00
               )

    # 5. We return res
    return res


def process_line_strings(line):
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
def ex1(sc=pyspark.SparkContext(), my_dataset_dir=""):
    print(sc.textFile(my_dataset_dir).count())


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir, station_number):
    rdd = sc.textFile(my_dataset_dir).persist()
    count = rdd.map(lambda x: process_line(x)).filter(lambda x: x[0] == station_number).groupBy(lambda x: x[4]).count()
    print(count)


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir, station_number):
    rdd = sc.textFile(my_dataset_dir)
    rows = rdd.map(lambda x: process_line(x)).filter(lambda x: x[0] == station_number).persist()

    partition1 = rows.map(lambda x: ("Behind Schedule", 1) if x[6] < x[7] else (0, 0))
    partition2 = rows.map(lambda x: ("Ahead Schedule", 1) if x[6] >= x[7] else (0, 0))

    partition1 = partition1.reduceByKey(lambda x, y: x + y)
    partition2 = partition2.reduceByKey(lambda x, y: x + y)

    result = partition1.collect()
    result2 = partition2.collect()

    print((result2[1][1], result[1][1]))


# ------------------------------------------
# FUNCTION my_sort
# ------------------------------------------
def my_sort(my_list):
    pass


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir, station_number):
    rdd = sc.textFile(my_dataset_dir).persist()

    cit_schedules = rdd.map(lambda x: process_line_strings(x)).filter(lambda x: x[0] == station_number).sortBy(
        lambda x: (datetime.strptime(x[4], "%d/%m/%Y")))  # 01/09/2016
    # cit_schedules = rdd.map(lambda x: process_line_strings(x)).filter(lambda x: x[0] == station_number).sortBy(
    #     lambda x: (datetime.strptime(x[7], "%H:%M:%S"), datetime.strptime(x[4], "%d/%m/%Y")))
    distinct_schedules = cit_schedules.map(lambda x: (x[3], x[6])).distinct().groupByKey()

    result = distinct_schedules.collect()

    for r in result:
        print((r[0], sorted(r[1])))


# ------------------------------------------
# FUNCTION time_to_int
# ------------------------------------------
def time_to_int(my_time):
    pass


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc, my_dataset_dir, station_number, month_list):
    # sc = pyspark.SparkContext()
    rdd = sc.textFile(my_dataset_dir).persist()
    rdd = rdd.map(lambda x: process_line(x))

    # get all rows for semester1 months for this station_number
    semester1_months_waiting = rdd.filter(lambda x: x[4].month >= 9 and x[4].month < 12 and x[0] == station_number)

    def get_waiting_time(x, y):
        return (y - x).total_seconds()

    waiting_time = semester1_months_waiting.map(lambda x: (x, get_waiting_time(x[5], x[7])))

    def get_day_and_month_string(x, y):
        return x + " " + ("%02d" % y.month)

    day_of_month = waiting_time.map(lambda x: (get_day_and_month_string(x[0][3], x[0][4]), x[1])) \
        .aggregateByKey((0, 0),
                        lambda x, y: (x[0] + y, x[1] + 1),
                        lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1])) \
        .sortBy(lambda x: x[1])

    results = day_of_month.collect()

    for res in results:
        print(res)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(sc, my_dataset_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(sc, my_dataset_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(sc, my_dataset_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(sc, my_dataset_dir, 240491, ['09', '10', '11'])


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
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    # my_databricks_path = "/"
    my_local_path = ""
    my_databricks_path = ""

    # my_dataset_dir = "FileStore/tables/7_Assignments/A02/my_dataset_single_file/"
    # my_dataset_dir = "../my_dataset_complete"
    my_dataset_dir = "../my_dataset_single_file"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    start = time()
    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option)
    finish = time() - start
    print("\n\nTime taken %f seconds" % finish)
