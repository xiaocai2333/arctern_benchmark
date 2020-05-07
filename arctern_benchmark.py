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
import json
import yaml

from runner.gis import single_col, st_point, double_col, st_polygon_from_envelope, st_geom_from_geojson
from arctern_pyspark import register_funcs, union_aggr, envelope_aggr
from pyspark.sql import SparkSession


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-s, --scheduler', dest='scheduler', nargs=1, default=None)

    args = parse.parse_args()

    if args.scheduler is not None:
        scheduler_file = args.scheduler[0]
    else:
        scheduler_file = "scheduler/gis_only/gis_test.txt"

    spark_session = SparkSession \
        .builder \
        .appName("arctern benchmark") \
        .getOrCreate()

    spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    register_funcs(spark_session)

    with open("scheduler/" + scheduler_file, "r") as f:
        line = f.readline()
        json_file = line.split(" ")[0]
        output_file = line.split(" ")[:-1]

    with open("test_case/" + json_file, "r") as f:
        bench_case = json.load(f)["gis_all"]

    for case in bench_case:
        csv_file_path = bench_case[case]["file_path"]
        # if hadoop is not None:
        #     csv_file_path = hadoop + "/" + csv_file_path

        function_sql_list = bench_case[case]["funcs_sql"]
        if case in ["single_col", "single_polygon", "single_point", "single_linestring", "st_curve_to_line"]:
            single_col.main(spark_session, csv_file_path, function_sql_list)
        elif case in ["double_col", "st_distance", "st_within"]:
            double_col.main(spark_session, csv_file_path, function_sql_list)
        elif case in ["st_point"]:
            st_point.main(spark_session, csv_file_path, function_sql_list)
        elif case in ["st_polygon_from_envelope"]:
            st_polygon_from_envelope.main(spark_session, csv_file_path, function_sql_list)
        elif case in ["st_geom_from_json"]:
            st_geom_from_geojson.main(spark_session, csv_file_path, function_sql_list)

    spark_session.stop()
    # Todo: read path and func_list from file and run benchmark
