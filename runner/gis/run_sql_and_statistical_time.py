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

import os
import time


def run_sql(spark, func_name, sql):
    begin_time = time.time()
    result_df = spark.sql(sql % func_name)
    result_df.createOrReplaceTempView("result")
    spark.sql("CACHE TABLE result")
    spark.sql("UNCACHE TABLE result")
    end_time = time.time()
    output_path = "./output/"
    print(func_name)
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    with open(output_path + "/" + func_name + ".txt", "a+") as f:
        f.write(" run sql " + sql % func_name + " time is : " + str((end_time - begin_time)/1000.0) + "s")


