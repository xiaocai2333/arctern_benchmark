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
import csv
import json
import pandas as pd
import re


def main(json_path):
    print("json_path = ", json_path)
    with open(json_path, "r") as f:
        test_cases = json.load(f)["gis_all"]

    for case in test_cases:
        csv_path = test_cases[case]["file_path"]
        func_sql_list = test_cases[case]["funcs_sql"]
        for fun_sql in func_sql_list:
            func_name = fun_sql[0]
            print("python func: ", func_name)
            if func_name in ["ST_Area", "ST_AsText", "ST_IsSimple", "ST_Length", "ST_GeometryType", "ST_IsValid",
                             "ST_AsGeoJSON", "ST_NPoints"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern." + func_name + "(arctern.ST_GeomFromText(data))"
                exec(exec_str)
            elif func_name in ["ST_Envelope", "ST_Centroid", "ST_ConvexHull", "ST_LineStringFromText", "ST_MakeValid",
                               "ST_CurveToLine", "ST_GeomFromGeoJSON", "ST_Union_Aggr", "ST_Envelope_Aggr"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(arctern.ST_GeomFromText(data)))"
                exec(exec_str)
            elif func_name in ["ST_GeomFromText"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(data))"
                exec(exec_str)
            elif func_name in ["ST_Contains", "ST_Crosses", "ST_Equals", "ST_HausdorffDistance", "ST_Intersects",
                               "ST_Overlaps", "ST_Touches", "ST_Distance", "ST_Within", "ST_DistanceSphere"]:
                data1 = []
                data2 = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data1.append(row[0])
                        data2.append(row[1])
                data1 = pd.Series(data1)
                data2 = pd.Series(data2)
                exec_str = "arctern." + func_name + "(arctern.ST_GeomFromText(data1), arctern.ST_GeomFromText(data2))"
                exec(exec_str)
            elif func_name in ["ST_Intersection", "ST_Point"]:
                data1 = []
                data2 = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data1.append(row[0])
                        data2.append(row[1])
                data1 = pd.Series(data1)
                data2 = pd.Series(data2)
                exec_str = "arctern.ST_AsText(arctern." + func_name + \
                           "(arctern.ST_GeomFromText(data1), arctern.ST_GeomFromText(data2)))"
                exec(exec_str)
            elif func_name in ["ST_PrecisionReduce"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(arctern.ST_GeomFromText(data), 3))"
                exec(exec_str)
            elif func_name in ["ST_SimplifyPreserveTopology"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(arctern.ST_GeomFromText(data), 10))"
                exec(exec_str)
            elif func_name in ["ST_Buffer"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(arctern.ST_GeomFromText(data), 1.2))"
                exec(exec_str)
            elif func_name in ["ST_TransFrom"]:
                data = []
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        data.append(row[0])
                data = pd.Series(data)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(arctern.ST_GeomFromText(data)," \
                                                                      " \"EPSG:4326\", \"EPSG:3857\"))"
                exec(exec_str)
            elif func_name in ["ST_PolygonFromEnvelope"]:
                x_min =[]
                y_min =[]
                x_max =[]
                y_max =[]
                with open(csv_path, newline='') as csv_file:
                    spamreader = csv.reader(csv_file, delimiter="|", quotechar="|")
                    for row in spamreader:
                        x_min.append(float(row[0]))
                        y_min.append(float(row[1]))
                        x_max.append(float(row[2]))
                        y_max.append(float(row[3]))
                x_min = pd.Series(x_min)
                y_min = pd.Series(y_min)
                x_max = pd.Series(x_max)
                y_max = pd.Series(y_max)
                exec_str = "arctern.ST_AsText(arctern." + func_name + "(x_min, y_min, x_max, y_max))"
                exec(exec_str)
