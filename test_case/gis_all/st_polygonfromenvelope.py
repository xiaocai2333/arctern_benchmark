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

csv_path = "data/st_polygon_from_envelope.csv"
col_num = 4

def data_proc(csv_path):
    import csv
    import pandas as pd
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


def run(x_min, y_min, x_max, y_max):
    arctern.ST_AsText(arctern.ST_PolygonFromEnvelope(x_min, y_min, x_max, y_max))
    print("ST_PolygonFromEnvelope run done!")
