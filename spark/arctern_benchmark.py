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
import importlib
from pyspark.sql import SparkSession
from arctern_pyspark import register_funcs


def run_sql(spark, sql, func_name):
    result_df = spark.sql(sql % func_name)
    result_df.createOrReplaceTempView("result")
    spark.sql("cache table result")
    spark.sql("uncache table result")


if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument('-s --source_file', dest='source_file', nargs=1)

    args = parse.parse_args()
    source_file = args.source_file[0]
    user_module = importlib.import_module("test_case." + (source_file.split(".")[0]).replace("/", "."),
                                          "test_case/" + source_file)
    spark_session = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()
    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    register_funcs(spark_session)

    if hasattr(user_module, "spark_test"):
        user_module.spark_test(spark_session)
    else:
        data_df = spark_session.read.format("csv").option("header", False).option("delimiter", "|").schema(
            user_module.schema).load(user_module.csv_path).cache()
        data_df.createOrReplaceTempView(user_module.func_name)
        result_df = spark_session.sql(user_module.sql % (user_module.col_name, user_module.func_name))
        result_df.createOrReplaceTempView("result")
        spark_session.sql("cache table result")
        spark_session.sql("uncache table result")
        print(user_module.func_name + " spark test run done!")
