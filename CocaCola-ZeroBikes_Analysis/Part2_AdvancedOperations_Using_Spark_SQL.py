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
import pyspark.sql.functions as functions
from pyspark.sql.functions import hour, minute, to_timestamp
from time import time

sql_session = pyspark.sql.SparkSession.builder.getOrCreate()


def my_parser(my_string):
    fields = my_string.split(";")
    new_row = pyspark.sql.Row(status=fields[0], name=fields[1], longitude=fields[2], latitude=fields[3],
                              date_status=fields[4], bikes_available=fields[5], docks_available=fields[6])

    # new_row = pyspark.sql.Row(status=fields[0], name=fields[1], longitude=fields[2], latitude=fields[3],
    #                           date_status=pyspark.sql.functions.to_timestamp(fields[4], "dd-MM-yyyy HH:mm:ss"),
    #                           bikes_available=fields[5], docks_available=fields[6])

    return new_row


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
        res = (int(params[0]),
               str(params[1]),
               float(params[2]),
               float(params[3]),
               # str(params[4]),
               pyspark.sql.functions.to_timestamp(params[4], "dd-MM-yyyy HH:mm:ss"),
               int(params[5]),
               int(params[6])
               )

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    inputRdd = sc.textFile(my_dataset_dir)
    rowRdd = inputRdd.map(my_parser)
    sqlDf = sql_session.createDataFrame(rowRdd)

    sqlDf.registerTempTable("df")

    result = sql_session.sql(
        "select name, count(name) as count from df where status = 0 and bikes_available = 0 group by name order by count desc")
    rows = result.collect()

    print(len(rows))
    for row in rows:
        print("(\"%s\"" % row["name"] + ", %d)" % row["count"])


# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir):
    inputRdd = sc.textFile(my_dataset_dir)
    rowRdd = inputRdd.map(my_parser)
    sqlDf = sql_session.createDataFrame(rowRdd)

    sqlDf.registerTempTable("df")

    results = sql_session.sql(
        "select name, sum(bikes_available) as sum from df where status = 0 and date_status like '%27-08-2017%' group by name order by sum desc").collect()

    for result in results:
        print("(\"%s\"" % result["name"] + ", %d)" % result["sum"])


# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir):
    inputRdd = sc.textFile(my_dataset_dir)
    rowRdd = inputRdd.map(my_parser)
    sqlDf = sql_session.createDataFrame(rowRdd)

    sqlDf.registerTempTable("df")
    sqlDf = sql_session.sql("select * from df where status = 0 and date_status like '%27-08-2017%'")

    parsed_datetime = sqlDf.select(sqlDf["name"], sqlDf["status"],
                                   hour(to_timestamp(sqlDf["date_status"], "dd-MM-yyyy HH:mm:ss")).alias("hr"),
                                   sqlDf["bikes_available"])

    results = parsed_datetime.groupBy(parsed_datetime["name"], parsed_datetime["hr"]).agg(
        functions.mean(parsed_datetime["bikes_available"]).alias("avg")).orderBy(parsed_datetime["name"],
                                                                                 parsed_datetime["hr"]).collect()

    for result in results:
        print("(\"%s" % result["name"] + " %02d" % result["hr"] + "\", %f)" % result["avg"])


# ------------------------------------------
# FUNCTION get_ran_outs
# ------------------------------------------
def get_ran_outs(my_list):
    pass


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir):
    inputRdd = sc.textFile(my_dataset_dir)
    rowRdd = inputRdd.map(my_parser)
    sqlDf = sql_session.createDataFrame(rowRdd)

    new_rows = []

    sqlDf.registerTempTable("df")
    sqlDf = sql_session.sql(
        "select status, name, longitude, latitude, to_timestamp(date_status, 'dd-MM-yyyy HH:mm:ss') as time, bikes_available, docks_available from df where status = 0 and date_status like '%27-08-2017%' order by name, time")

    rows = list(sqlDf.toLocalIterator())

    station_name = ""
    flag = True

    for row in rows:
        # print("\n\n----------------------------------iterating row \n%s" % str(row))
        current_station = row['name']
        # print("----------------------------------current station \n%s" % current_station)
        if current_station != station_name:
            station_name = current_station
            flag = True
            # print("----------------------------------changing station \n%s" % station_name)

        if flag and int(row['bikes_available']) == 0:
            # print("----------------------------------found first occurence with 0 bikes")
            new_rows.append(row)
            flag = False
        elif not flag and int(row['bikes_available']) > 0:
            # print("----------------------------------found first station occurence with refilled bikes")
            flag = True

    # put it into the Dataframe
    newDf = sql_session.createDataFrame(new_rows)

    # sort just based on time
    newDf = newDf.orderBy(["time"])

    # print in appropriate format
    newDf = newDf.withColumn("hour", hour(newDf["time"]))
    newDf = newDf.withColumn("minute", minute(newDf["time"]))

    results = newDf.collect()

    for result in results:
        print("(\'%02d" % result["hour"] + ":%02d\'," % result["minute"] + "\'%s\')" % result["name"])


# ------------------------------------------
# FUNCTION my_update_accum
# ------------------------------------------
def my_update_accum(accum, item):
    pass


# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc, my_dataset_dir, ran_out_times):
    inputRdd = sc.textFile(my_dataset_dir)
    rowRdd = inputRdd.map(my_parser)
    sqlDf = sql_session.createDataFrame(rowRdd)

    sqlDf.registerTempTable("df")
    sqlDf = sql_session.sql(
        "select status, name, longitude, latitude, to_timestamp(date_status, 'dd-MM-yyyy HH:mm:ss') as time, bikes_available, docks_available from df where status = 0 and date_status like '%27-08-2017%' order by name, time")

    # re-registered the table because column renamed from "date_status" to "time"
    sqlDf.registerTempTable("df")

    # done - record time taken without persist and with persist
    # approx 16 seconds with persistence
    # approx 184 seconds without persistence
    sqlDf.persist()

    best_stations = []
    for ran_out_time in ran_out_times:
        # select all stations where time like ran_out_time
        query = "select * from df where time like \'%" + ran_out_time + "%\' order by int(bikes_available) desc, longitude asc, latitude asc"
        stations_df_for_time = sql_session.sql(query)

        best_station = stations_df_for_time.first()
        best_stations.append(best_station)

    best_stations_df = sql_session.createDataFrame(best_stations)
    best_stations_df = best_stations_df.withColumn("hour", hour(best_stations_df["time"]))
    best_stations_df = best_stations_df.withColumn("minute", minute(best_stations_df["time"]))

    results = best_stations_df.collect()

    for result in results:
        print(
            "(\'%02d" % result["hour"] + ":%02d\'," % result["minute"] + "\'(%s\'" % result["name"] + ", %s))" % result[
                "bikes_available"])


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option, ran_out_times):
    # Exercise 1: Number of times each station ran out of bikes (sorted decreasingly by station).
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Total amount of bikes availables being measured per station (sorted decreasingly by number of bikes)
    if option == 2:
        ex2(sc, my_dataset_dir)

    # Exercise 3: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Average amount of bikes per station and hour window (e.g. [9am, 10am), [10am, 11am), etc. )
    if option == 3:
        ex3(sc, my_dataset_dir)

    # Exercise 4: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the different ran-outs to attend.
    #             Note: n consecutive measurements of a station being ran-out of bikes has to be considered a single ran-out,
    #                   that should have been attended when the ran-out happened in the first time.
    if option == 4:
        ex4(sc, my_dataset_dir)

    # Exercise 5: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the station with biggest number of bikes for each ran-out to be attended.
    if option == 5:
        ex5(sc, my_dataset_dir, ran_out_times)


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

    ran_out_times = ['06:03:00', '06:03:00', '08:58:00', '09:28:00', '10:58:00', '12:18:00',
                     '12:43:00', '12:43:00', '13:03:00', '13:53:00', '14:28:00', '14:28:00',
                     '15:48:00', '16:23:00', '16:33:00', '16:38:00', '17:09:00', '17:29:00',
                     '18:24:00', '19:34:00', '20:04:00', '20:14:00', '20:24:00', '20:49:00',
                     '20:59:00', '22:19:00', '22:59:00', '23:14:00', '23:44:00']

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_local_path = ""
    my_databricks_path = "/"

    # my_dataset_dir = "FileStore/tables/7_Assignments/A01/my_dataset/"
    my_dataset_dir = "./data/my_dataset/"
    # my_dataset_dir = "./data/my_tiny_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option, ran_out_times)
