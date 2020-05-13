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

import csv
import time
import pandas as pd
import inspect


def data_proc(csv_path, col_num):
    data_list = []
    with open(csv_path, "r") as csv_file:
        spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
        for i in range(col_num):
            data = []
            for row in spreader:
                if col_num == 4:
                    data.append(float(row[i]))
                else:
                    data.append(row[i])
            data_list.append(pd.Series(data))
    return data_list


def python_test(output_file, user_module):

    if not hasattr(user_module, "python_test"):
        print("Please write python_test function in your %s!" % str(user_module))

    if hasattr(user_module, "data_proc"):
        data = user_module.data_proc(user_module.csv_path, user_module.col_num)
    else:
        data = data_proc(user_module.csv_path, user_module.col_num)
    begin_time = time.time()
    user_module.python_test(*data)
    end_time = time.time()

    with open(output_file, "w") as out:
        out.writelines("run " + str(user_module) + " time is: " + str(end_time - begin_time) + "s")
