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

import builtins
import csv
import json
import time
import pandas as pd
import inspect
import sys


def TIME_START(step):
    stack = inspect.stack()
    c = stack[-2][0]
    c_module = inspect.getmodule(c)
    if not hasattr(c_module, "__timeinfo"):
        setattr(c_module, "timeinfo", {})
    c_module.timeinfo[step] = time.time()


def TIME_END(step):
    stack = inspect.stack()
    c = stack[-2][0]
    c_module = inspect.getmodule(c)
    assert hasattr(c_module, "timeinfo")
    start = c_module.timeinfo[step]
    dur = round(time.time() - start, 4)
    c_module.timeinfo[step] = dur


def TIME_INFO():
    stack = inspect.stack()
    c = stack[-2][0]
    c_module = inspect.getmodule(c)
    ret = c_module.timeinfo
    return ret


setattr(builtins, "TIME_START", TIME_START)
setattr(builtins, "TIME_END", TIME_END)
setattr(builtins, "TIME_INFO", TIME_INFO)


def data_proc(csv_path, col_num):
    data_list = []
    for i in range(col_num):
        locals()["data_%s" % i] = []
    with open(csv_path, "r") as csv_file:
        spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
        for row in spreader:
            for i in range(col_num):
                if col_num == 4:
                    locals()["data_%s" % i].append(float(row[i]))
                else:
                    locals()["data_%s" % i].append(row[i])
            # print(data)
    for i in range(col_num):
        data_list.append(pd.Series(locals()["data_%s" % i]))
    return data_list


def python_test(output_file, user_module, run_time, commit_id):

    if not hasattr(user_module, "python_test"):
        print("Please write python_test function in your %s!" % user_module.__name__)
        sys.exit(0)

    if hasattr(user_module, "data_proc"):
        data = user_module.data_proc()
    else:
        data = data_proc(user_module.csv_path, user_module.col_num)

    all_time_info = {"version": commit_id.split("-")[0], "commit_id": commit_id.split("-")[-1],
                     "func_name": user_module.func_name}
    for times in range(run_time):
        time_info = {}
        begin_time = time.time()
        time_info["step"] = user_module.python_test(*data)
        end_time = time.time()
        time_info["total_time"] = round(end_time - begin_time, 4)
        all_time_info["%s" % str(times)] = time_info

    with open(output_file, "w") as out:
        json_obj = json.dumps(all_time_info)
        out.write(json_obj)
