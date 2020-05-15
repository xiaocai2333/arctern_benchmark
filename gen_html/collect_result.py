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
import json


def read_file_calculate_time(file):
    total_time = []
    with open(file, "r") as f:
        file_data = json.load(f)
        for key in file_data:
            try:
                int(key)
                total_time.append(file_data[key])
            except ValueError:
                pass
    s = 0
    for time in total_time[1:]:
        s += time
    return s / (len(total_time) - 1)


def extract_all_pref():
    all_version = []
    all_version_path = []
    for d in os.listdir("output"):
        if os.path.isdir(os.path.join("output", d)):
            all_version.append(d)
            all_version_path.append(os.path.join("output", d))
    all_commit_id = []
    all_commit_id_path = []
    for version in all_version_path:
        for d in os.listdir(version):
            if os.path.isdir(os.path.join(version, d)):
                all_commit_id.append(d)
                all_commit_id_path.append(os.path.join(version, d))

    python_output_path = [os.path.join(commit_id_path, "python") for commit_id_path in all_commit_id_path]
    spark_output_path = [os.path.join(commit_id_path, "spark") for commit_id_path in all_commit_id_path]
    test_suites = [path for path in os.listdir(python_output_path[0]) if os.path.isdir(os.path.join(python_output_path[0], path))]
    return all_version, all_commit_id, python_output_path, spark_output_path, test_suites


def pref_data(output_path):
    all_version, all_commit_id, python_output_path, spark_output_path, test_suites = extract_all_pref()
    print(all_version)
    print(all_commit_id)
    print(python_output_path)
    print(spark_output_path)
    print(test_suites)   # all test cases for example([case1, case2, case3])
    all_case_file = {}
    # all test cases and all files for each case for example
    # ({"gis_test":[st_area.txt, st_buffer.txt], "gis_all":[case1, case2, case3]})
    for test_case in test_suites:
        case_files = [file for file in os.listdir(os.path.join(python_output_path[0], test_case)) if os.path.isfile(
            os.path.join(os.path.join(python_output_path[0], test_case), file))]
        all_case_file[test_case] = case_files

    print(test_suites)
    spark_total_time = []
    python_total_time = []

    all_case_time = []
    for test_case in test_suites:
        case_files = test_suites[test_case]
        pre_case_all_file_time = []
        for file in case_files:
            pre_file_time = []
            for path in python_output_path:
                pre_file_time.append(read_file_calculate_time(os.path.join(os.path.join(path, test_case), file)))
            pre_case_all_file_time.append(pre_file_time)
        all_case_time.append(pre_case_all_file_time)

    func_time_list = []
    for i in range(len(spark_total_time)):
        spark_time = spark_total_time[i]
        python_time = python_total_time[i]
        func_time = str(spark_time) + ":" + str(python_time)
        func_time_list.append(func_time)

    html_data = {'REP_NODES':all_commit_id, 'REP_SET_NAMES':["spark", "python"], 'REP_DATASETS':func_time_list,
                 'REP_FUNC_NAMES': file_list}

    with open("./a.txt", "w") as f:
        json_obj = json.dumps(html_data)
        f.write(json_obj)
    # return html_data



