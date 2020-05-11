# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import csv
import importlib
import pandas as pd
import time
import os
import sys


def data_proc(csv_path, col_num):
    if col_num == 1:
        data = []
        with open(csv_path, "r") as csv_file:
            spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
            for row in spreader:
                data.append(row[0])
        return pd.Series(data)
    elif col_num == 2:
        data1 = []
        data2 = []
        with open(csv_path, "r") as csv_file:
            spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
            for row in spreader:
                data1.append(row[0])
                data2.append(row[1])
        return pd.Series(data1), pd.Series(data2)
    elif col_num == 4:
        x_min = []
        x_max = []
        y_min = []
        y_max = []
        with open(csv_path, "r") as csv_file:
            spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
            for row in spreader:
                x_min.append(float(row[0]))
                y_min.append(float(row[1]))
                x_max.append(float(row[2]))
                y_max.append(float(row[3]))
        return pd.Series(x_min), pd.Series(y_min), pd.Series(x_max), pd.Series(y_max)


def run_sql_once(spark, sql):
    result_df = spark.sql(sql)
    result_df.createOrReplaceTempView("result")
    spark.sql("cache table result")
    spark.sql("uncache table result")


def spark_test(output_file, user_module):
    from pyspark.sql import SparkSession
    from arctern_pyspark import register_funcs
    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark_session.conf.set("spark.executor.memory", "2g")

    register_funcs(spark_session)

    if hasattr(user_module, "spark_test"):
        begin_time = time.time()
        user_module.spark_test(spark_session)
        end_time = time.time()
    else:
        data_df = spark_session.read.format("csv").option("header", False).option("delimiter", "|").schema(
            "geos string").load(user_module.csv_path).cache()
        data_df.createOrReplaceTempView(user_module.func_name)
        begin_time = time.time()
        run_sql_once(spark_session, user_module.sql)
        end_time = time.time()
        print("run " + user_module + " time is:" + str(end_time-begin_time) + "s")

    with open(output_file, "w") as out:
        out.writelines("run " + user_module + " time is: " + str(end_time - begin_time) + "s")


def python_test(output_file, user_module):
    begin_time = time.time()
    end_time = time.time()

    if user_module.col_num == 1:
        if hasattr(user_module, "data_proc"):
            data = user_module.data_proc(user_module.csv_path, user_module.col_num)
        else:
            data = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.run(data)
        end_time = time.time()

    elif user_module.col_num == 2:
        if hasattr(user_module, "data_proc"):
            data1, data2 = user_module.data_proc(user_module.csv_path)
        else:
            data1, data2 = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.run(data1, data2)
        end_time = time.time()

    elif user_module.col_num == 4:
        if hasattr(user_module, "data_proc"):
            x_min, y_min, x_max, y_max = user_module.data_proc(user_module.csv_path)
        else:
            x_min, y_min, x_max, y_max = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.run(x_min, y_min, x_max, y_max)
        end_time = time.time()

    with open(output_file, "w") as out:
        out.writelines("run " + user_module + " time is: " + str(end_time - begin_time) + "s")


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    # parse.add_argument('-l --last_version', dest='last_version', nargs='+')
    # parse.add_argument('-c --current_version', dest='current_version', nargs='+')
    parse.add_argument('-v --arctern_version', dest='arctern_version', nargs=1)
    parse.add_argument('-p --python', dest="python", nargs='+')
    parse.add_argument('-s --spark', dest="spark", nargs='+')

    args = parse.parse_args()
    scheduler_file = "scheduler/gis_only/gis_test.txt"
    if args.file is not None:
        scheduler_file = args.scheduler[0]

    # Todo: create conda environment by yaml
    # Todo: read arctern version by web and write conda yaml

    output_path = "./output/0.1.0/"
    if args.arctern_version is None:
        print("Please input artern version yaml")
        sys.exit(0)
    else:
        arctern_version = args.arctern_version
        output_path = "./output/" + arctern_version

    with open(scheduler_file, "r") as f:
        for line in f:
            source_file = line.split(" ")[0]
            output_file = line.split(" ")[1]

            test_case = output_file.replace(output_file.split("/")[-1], "")
            out_python_path = output_path + "/python/" + test_case
            out_spark_path = output_path + "/spark/" + test_case
            if not os.path.exists(out_python_path):
                os.makedirs(out_python_path)
            if not os.path.exists(out_spark_path):
                os.makedirs(out_spark_path)

            user_module = importlib.import_module("test_case." + (source_file.split(".")[0]).replace("/", "."),
                                                  "test_case/" + source_file)

            if args.spark is not None:
                spark_test(out_spark_path + output_file.split("/")[-1], user_module)

            if args.python is not None:
                python_test(out_python_path + "/" + output_file.split("/")[-1], user_module)











