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
                total_time.append(file_data[key]["total_time"])
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


def pref_data():

    all_version, all_commit_id, python_output_path, spark_output_path, test_suites = extract_all_pref()

    all_case_files = {}
    # all test cases and all files for each case for example
    # ({"gis_test":[st_area.txt, st_buffer.txt], "gis_all":[case1, case2, case3]})
    for test_case in test_suites:
        case_files = [file.replace(".json", "") for file in os.listdir(os.path.join(python_output_path[0], test_case)) if os.path.isfile(
            os.path.join(os.path.join(python_output_path[0], test_case), file))]
        all_case_files[test_case] = case_files

    all_case_time = []
    for test_case in all_case_files:
        case_files = all_case_files[test_case]
        pre_case_all_file_time = []
        for file in case_files:
            python_time = []
            spark_time = []
            for i in range(len(python_output_path)):
                python_file = os.path.join(os.path.join(python_output_path[0], test_case), file + ".json")
                spark_file = os.path.join(os.path.join(spark_output_path[0], test_case), file + ".json")
                python_time.append(str(read_file_calculate_time(python_file)))
                spark_time.append(str(read_file_calculate_time(spark_file)))
            pre_file_time = ",".join(python_time) + ":" + ",".join(spark_time)
            pre_case_all_file_time.append(pre_file_time)
        all_case_time.append(pre_case_all_file_time)

    return all_version, all_commit_id, all_case_time, all_case_files


def gen_data_path():
    all_version, all_commit_id, all_case_time, all_case_files = pref_data()
    for i in range(len(all_case_files)):
        file_dict = {"REP_NODES": all_commit_id, "REP_SET_NAMES": ["python", "spark"], "REP_DATASETS": all_case_time[i],
                     "REP_FUNC_NAMES": all_case_files[list(all_case_files.keys())[i]]}
        with open("gen_html/data_path/" + list(all_case_files.keys())[i] + ".txt", "w") as data_f:
            data_f.write(str(file_dict))

