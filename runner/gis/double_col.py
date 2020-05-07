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

from runner.gis import run_sql_and_statistical_time


def run_double_col(spark, file_path, function_sql_list):
    df = spark.read.format("csv").option("header", False).option("delimiter", "|").schema(
        "left string, right string").load(file_path).cache()
    for i in range(len(function_sql_list)):
        func_name = function_sql_list[i][0]
        sql = function_sql_list[i][1]
        df.createOrReplaceTempView(func_name)
        if len(sql) == 1:
            run_sql_and_statistical_time.run_sql(spark, func_name, sql)

    df.unpersist()


def main(spark, csv_file_path, function_sql_list):

    run_double_col(spark, csv_file_path, function_sql_list)

    print("double col benchmark done!")
