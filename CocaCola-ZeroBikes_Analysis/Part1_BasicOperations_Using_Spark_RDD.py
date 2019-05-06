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
import time
import numpy as np

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
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. We count the total amount of entries
    count = inputRDD.count()

    # 3. We print the result
    print(count)


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. We process each line to get the relevant info
    # 3. We project just the info we are interested into
    onlyStationNamesRDD = inputRDD.map(lambda x: x.split(";")[1])

    # 4. We get just the entries that are different
    # 5. We count such these entries
    count = onlyStationNamesRDD.distinct().count()

    # 6. We print the result
    print(count)


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    # 2. We process each line to get the relevant info
    # 3. We project just the info we are interested into
    # 4. We get just the entries that are different
    # 5. We collect such these entries
    # 6. We print them
    for bike_station in sc.textFile(my_dataset_dir).\
            map(lambda x: x.split(";")[1]).\
            distinct()\
            .collect():
        print(bike_station)


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    inputRdd = sc.textFile(my_dataset_dir)

    # 2. We process each line to get the relevant info
    # 3. We project just the info we are interested into
    # 4. We get just the entries that are different
    # 5. We sort them by their longitude
    # 6. We collect such these entries
    results = inputRdd.map(lambda x: "('%s" % x.split(";")[1] + "', %s" % x.split(";")[2] + ")") \
        .distinct() \
        .sortBy(lambda x: x.split(", ")[1][:-1]) \
        .collect()

    # 7. We print them
    for result in results:
        print(result)


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc=pyspark.SparkContext(), my_dataset_dir=""):
    # sc = pyspark.SparkContext()
    # ex5_start_time = time.time()
    # 1. We load the dataset into an inputRDD
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. We process each line to get the relevant info
    # 3. We filter the bikes of "Kent Station"
    resultRDD = inputRDD.filter(lambda x: x.split(";")[1] == "Kent Station").map(lambda x: float(x.split(";")[5]))


    # 4. We project just the info we are interested into
    # result = resultRDD.collect()

    # 5. We compute the average amount of bikes
    the_sum = resultRDD.sum()
    the_mean = resultRDD.mean()
    the_count = resultRDD.count()
    # total_sum = np.sum(result)
    # total_items = len(result)
    # mean = total_sum/total_items

    # # 6. We print this info by the screen
    # print("TotalSum = %d" % total_sum)
    # print("TotalItems = %d" % total_items)
    # print("AverageValue = %f" % mean)

    print("TotalSum = %d" % the_sum)
    print("TotalItems = %d" % the_count)
    print("AverageValue = %f" % the_mean)
    # ex5_finish_time = time.time() - ex5_start_time
    # print("Ex5 Total Time: %.2f" %ex5_finish_time + " seconds")


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option):
    # Exercise 1: Total amount of entries in the dataset.
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Number of Coca-cola bikes stations in Cork.
    if option == 2:
        ex2(sc, my_dataset_dir)

    # Exercise 3: List of Coca-Cola bike stations.
    if option == 3:
        ex3(sc, my_dataset_dir)

    # Exercise 4: Sort the bike stations by their longitude (East to West).
    if option == 4:
        ex4(sc, my_dataset_dir)

    # Exercise 5: Average number of bikes available at Kent Station.
    if option == 5:
        ex5(sc, my_dataset_dir)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 2



    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_local_path = ""
    my_databricks_path = "/"

    # my_dataset_dir = "FileStore/tables/7_Assignments/A01/my_dataset/"
    # my_dataset_dir = "./data/my_micro_dataset/"
    my_dataset_dir = "./data/my_tiny_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")


    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option)
