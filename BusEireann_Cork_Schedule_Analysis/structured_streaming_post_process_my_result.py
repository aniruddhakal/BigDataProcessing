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

import os
import codecs
import shutil
import pathlib

# ------------------------------------------
# FUNCTION get_file_info
# ------------------------------------------
def get_file_info(my_file, step_files_tuples):
    # 1. We create the output variable
    res = []

    # 2. We open the file for reading
    my_input_stream = codecs.open(my_file, "r", encoding='utf-8')

    # 3. We discard the first line
    my_input_stream.readline()

    # 4. We read the rest of lines
    for line in my_input_stream:
        # 4.1. We split it by fields
        fields = line.split(",")

        # 4.2. We get the path
        file_name = fields[0]
        index = file_name.rfind("/")
        file_name = file_name[(index+1):]
        file_name = file_name[:-1]

        # 4.3. We get the timestamp
        timestamp = fields[3]
        index = timestamp.index(":")
        timestamp = timestamp[(index+1):]

        # 4.4. Extra: Files can be repeated
        repeated = False
        for zz_list in step_files_tuples:
            for my_tuple in zz_list:
                if my_tuple[0] == file_name:
                    repeated = True

        if repeated == False:
            # 4.5. If the file contains size > 0
            if (fields[1][-2:] != ":0"):
                res.append( (file_name, timestamp, True) )
            else:
                res.append( (file_name, timestamp, False) )

    # 5. We close the file
    my_input_stream.close()

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION get_metadata
# ------------------------------------------
def get_metadata(metadata_dir):
    # 1. We create the output variable
    res = []

    # 2. We get the list of batches metadata from metadata_dir
    files = os.listdir(metadata_dir)

    for item in files:
        if ".compact" in item:
            new_name = item.replace(".compact", "")
            os.rename(metadata_dir + item, metadata_dir + new_name)

    files = os.listdir(metadata_dir)

    # 3. We sort the batches metadata in order
    sorted_files = sorted(files, key=lambda item: (len(item), item), reverse=False)

    # 4. We get the file name and timestamp per batch file
    for file in sorted_files:
        if file[-4:] != ".crc":
            # 4.1. We get the info from the file
            files_info = get_file_info(metadata_dir + file, res)

            # 4.2. We append the list of files_info to res
            res.append(files_info)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION move_files
# ------------------------------------------
def move_files(dir, steps_metadata):
    # 1. We create the time_step
    time_step = 0

    # 2. We traverse all time steps to put the files in a dedicated folder
    for step in steps_metadata:
        # 2.1. We mark as no file with actual content being found yet
        content = False

        # 2.2. We traverse the files to copy them
        for file_info in step:
            # 2.2.1. If the file contains actual content
            my_file = pathlib.Path(dir + str(file_info[0]))

            if (my_file.exists()) and (file_info[2] == True):
                # 2.2.1.1. If it is the first file we find with this content
                if content == False:
                    # I. We get the new folder name
                    new_dir = dir + "Step" + str(time_step) + "/"

                    # II. We create the new directory
                    os.mkdir(new_dir)

                    # III. We increase time_step
                    time_step = time_step + 1

                    # IV. We set content to True
                    content = True

                # 2.2.1.2.  We copy the file from dir to new_dir
                shutil.copyfile(dir + str(file_info[0]), new_dir + str(file_info[0]))

            # 2.2.2. We remove the file from dir
            if my_file.exists():
                os.remove(my_file)

# ------------------------------------------
# FUNCTION merge_files
# ------------------------------------------
def merge_files(sol_name, dir_name):
    # 1. We create the solutionRDD
    content = []

    # 2. We collect all the files of the dir
    files = os.listdir(dir_name)

    # 3. We collect the content of each file
    for file_name in files:
        # 3.1. We open the file
        my_input_file = codecs.open(dir_name + file_name, "r", encoding='utf-8')

        # 3.2. We read it line by line
        for line in my_input_file:
            content.append(line)

        # 3.3. We close the input file
        my_input_file.close()

    # 4. We avoid empty time_steps: If any actual line has been read at all
    if (len(content) > 0):
        # 4.1. We open the file for writing
        my_output_file = codecs.open(dir_name + sol_name, "w", encoding='utf-8')

        # 4.2. We write the content to the file
        for line in content:
            my_output_file.write(line)

        # 4.3. We close the output file
        my_output_file.close()

# ------------------------------------------
# FUNCTION generate_step_solution_file
# ------------------------------------------
def generate_step_solution_file(sol_name, my_result_dir):
    # 1. We collect all the sub-directories names (there is one sub-directory per time_interval)
    files = os.listdir(my_result_dir)

    # 2. We filter just the directories
    directories = [item for item in files if (os.path.isdir(my_result_dir + item) == True)]

    directories.sort()

    # 2. We traverse each sub-directory (time_interval)
    for item in directories:
        if (item != "_spark_metadata"):
            # 2.1. We get the name of the sub-directory
            dir_name = my_result_dir + item + "/"

            # 2.2. We merge the parts of such sub-directory
            merge_files(sol_name, dir_name)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dir):
    # 1. We get the names and timestamp in order
    steps_metadata = get_metadata(dir + "_spark_metadata/")

    # 2. We create the subfolders directory
    move_files(dir, steps_metadata)

    # 3. Amalgamate files per step
    generate_step_solution_file("solution.csv", dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or Databricks
    local_False_databricks_True = False

    # 2. We set the path to my_dataset and my_result
    # my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    # my_databricks_path = "/"
    my_local_path = ""
    my_databricks_path = ""

    # result_dir = "FileStore/tables/3_Spark_SQL/3_2_Structured_Streaming/my_result/"
    result_dir = "../streaming_dirs/result/"

    if local_False_databricks_True == False:
        result_dir = my_local_path + result_dir
    else:
        result_dir = my_databricks_path + result_dir

    # 3. We call to my_main
    my_main(result_dir)
