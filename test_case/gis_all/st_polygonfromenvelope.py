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

func_name = "st_polygon_from_envelope"
csv_path = "data/st_polygon_from_envelope.csv"
col_num = 4
col_name = ["x_min", "y_min", "x_max", "y_max"]
schema = "x_min double, y_min double, x_max double, y_max double"

sql = "select ST_AsText(ST_PolygonFromEnvelope(%s, %s, %s, %s)) from %s"


def data_proc():
    import csv
    import pandas as pd
    x_min = []
    x_max = []
    y_min = []
    y_max = []
    data = []
    with open(csv_path, "r") as csv_file:
        spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
        for row in spreader:
            x_min.append(float(row[0]))
            y_min.append(float(row[1]))
            x_max.append(float(row[2]))
            y_max.append(float(row[3]))
    data.append(pd.Series(x_min))
    data.append(pd.Series(y_min))
    data.append(pd.Series(x_max))
    data.append(pd.Series(x_max))
    return data


def python_test(x_min, y_min, x_max, y_max):
    TIME_START(func_name)
    arctern.ST_AsText(arctern.ST_PolygonFromEnvelope(x_min, y_min, x_max, y_max))
    TIME_END(func_name)

    return TIME_INFO()
