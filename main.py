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


def run_sql_multi(spark, sql):
    result_df = spark.sql(sql%"result")
    result_df.createOrReplaceTempView("result")
    spark.sql("cache table result")


def spark_test(scheduler_file):
    from pyspark.sql import SparkSession
    from arctern_pyspark import register_funcs
    with open(scheduler_file, "r") as f:
        for line in f:
            source_file = line.split(" ")[0]
            output_file = line.split(" ")[1]
            user_module = importlib.import_module("test_case." + (source_file.split(".")[0]).replace("/", "."),
                                                  "test_case/" + source_file)
            spark_session = SparkSession \
                .builder \
                .appName("Python Arrow-in-Spark example") \
                .getOrCreate()
            spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            spark_session.conf.set("spark.executor.memory", "2g")
            if hasattr(user_module, "spark_test"):
                user_module.spark_test(spark_session)
            else:
                data_df = spark_session.read.format("csv").option("header", False).option("delimiter", "|").schema(
                    "geos string").load(user_module.csv_path).cache()
            if len(user_module.sql_list) > 1:
                for sql in user_module.sql_list:
                    run_sql_multi(spark_session, sql)
            else:
                run_sql_once(spark_session, user_module.sql_list[0])


def python_test(scheduler_file):
    with open(scheduler_file, "r") as f:
        for line in f:
            source_file = line.split(" ")[0]
            output_file = line.split(" ")[1]
            user_module = importlib.import_module("test_case." + (source_file.split(".")[0]).replace("/", "."),
                                                  "test_case/" + source_file)
            if user_module.col_num == 1:
                if hasattr(user_module, "data_proc"):
                    data = user_module.data_proc(user_module.csv_path, user_module.col_num)
                else:
                    data = data_proc(user_module.csv_path, user_module.col_num)
                begin_time = time.time()
                user_module.run(data)
                end_time = time.time()
                if args.last_version:
                    output_path = "output/last_version/" + output_file
                    if not os.path.exists(output_path):
                        os.makedirs(output_path)
                    with open(output_path, "w") as out_f:
                        out_f.writelines("run " + user_module + " time is: " + str(end_time - begin_time) + "s")
                elif args.current_version:
                    output_path = "output/current_version/" + output_file
                    if not os.path.exists(output_path):
                        os.makedirs(output_path)
                    with open(output_path, "w") as out_f:
                        out_f.writelines("run " + user_module + " time is: " + str(end_time - begin_time) + "s")

            elif user_module.col_num == 2:
                if hasattr(user_module, "data_proc"):
                    data1, data2 = user_module.data_proc(user_module.csv_path)
                else:
                    data1, data2 = data_proc(user_module.csv_path, user_module.col_num)
                user_module.run(data1, data2)

            elif user_module.col_num == 4:
                if hasattr(user_module, "data_proc"):
                    x_min, y_min, x_max, y_max = user_module.data_proc(user_module.csv_path)
                else:
                    x_min, y_min, x_max, y_max = data_proc(user_module.csv_path, user_module.col_num)
                user_module.run(x_min, y_min, x_max, y_max)


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    parse.add_argument('-l --last_version', dest='last_version', nargs='+')
    parse.add_argument('-c --current_version', dest='current_version', nargs='+')
    parse.add_argument('-p --python', dest="python", nargs='+')
    parse.add_argument('-s --spark', dest="spark", nargs='+')

    args = parse.parse_args()
    scheduler_file = "scheduler/gis_only/gis_test.txt"
    if args.file is not None:
        scheduler_file = args.scheduler[0]

    if args.spark is not None:
        spark_test(scheduler_file)

    if args.python is not None:
        python_test(scheduler_file)










