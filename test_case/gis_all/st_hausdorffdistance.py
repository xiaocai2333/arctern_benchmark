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

func_name = "st_hausdorff_distance"
csv_path = "data/double_col.csv"
col_num = 2
col_name = ["left", "right"]
schema = "left string, right string"

sql = "select ST_HausdorffDistance(ST_GeomFromText(%s),ST_GeomFromText(%s)) from %s"


def python_test(data1, data2):
    TIME_START(func_name)
    arctern.ST_HausdorffDistance(arctern.ST_GeomFromText(data1), arctern.ST_GeomFromText(data2))
    TIME_END(func_name)

    return TIME_INFO()
