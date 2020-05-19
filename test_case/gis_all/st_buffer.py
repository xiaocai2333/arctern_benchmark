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


import arctern

csv_path = "data/single_point.csv"
func_name = "st_buffer"
col_num = 1
col_name = ["geos"]
schema = "geos string"
sql = "select ST_AsText(ST_Buffer(ST_GeomFromText(%s), 1.2)) from %s"


def spark_test(spark):
    TIME_START(func_name)
    result_df = spark.sql(sql % (*col_name, func_name))
    result_df.createOrReplaceTempView("result")
    spark.sql("cache table result")
    spark.sql("uncache table result")
    TIME_END(func_name)

    return TIME_INFO()


def python_test(data):
    TIME_START("st_buffer")
    arctern.ST_AsText(arctern.ST_Buffer(arctern.ST_GeomFromText(data), 1.2))
    TIME_END("st_buffer")
    return TIME_INFO()

