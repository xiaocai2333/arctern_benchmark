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
import csv
import importlib
import pandas as pd
import time
import os
import sys
import yaml


def switch_conda_environment(conda_environment_file, args):
    with open(conda_environment_file, "r") as env_f:
        yaml_conf = yaml.safe_load(env_f)
        dependencies = yaml_conf["dependencies"]
        version = dependencies[0].split("=")[1]
        commit_id = dependencies[0].split("=")[2].replace("*", "")

    conda_env_name = "arctern" + version + "-" + commit_id
    with open(conda_environment_file, "w") as env_f:
        yaml_conf["name"] = conda_env_name
        yaml_conf["channels"] = ["conda-forge", "arctern-dev"]
        yaml_conf["dependencies"].append("pyyaml")
        yaml.dump(yaml_conf, env_f)
    status = os.system("conda env create -f " + conda_environment_file)

    if status >= 0:
        conda_prefix = os.popen("conda env export -n %s | grep prefix" % conda_env_name).read().split(" ")[-1].replace(
            "\n", "")
        exec_python_path = conda_prefix + "/bin/python"
        for n, e in enumerate(sys.argv):
            if e == "-w":
                sys.argv[n + 1] = "False"
        os.execlp(exec_python_path, "arctern test", *sys.argv)
    else:
        print("create conda environment failed!")
        sys.exit(1)


def data_proc(csv_path, col_num):
    if col_num == 1:
        data = []
        with open(csv_path, "r") as csv_file:
            spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
            for row in spreader:
                data.append(row[0])
        return pd.Series(data)
    elif col_num == 2:
        data1 = []
        data2 = []
        with open(csv_path, "r") as csv_file:
            spreader = csv.reader(csv_file, delimiter="|", quotechar="|")
            for row in spreader:
                data1.append(row[0])
                data2.append(row[1])
        return pd.Series(data1), pd.Series(data2)
    elif col_num == 4:
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


def spark_test(output_file, user_module, source_file):
    from spark import arctern_benchmark
    begin_time = time.time()
    os.system("spark-submit ./spark/arctern_benchmark.py -s %s" % source_file)
    end_time = time.time()
    with open(output_file, "w") as out:
        out.writelines("run " + str(user_module) + " time is: " + str(end_time - begin_time) + "s")


def python_test(output_file, user_module):
    begin_time = time.time()
    end_time = time.time()

    if not hasattr(user_module, "python_test"):
        print("Please write python_test function in your %s!" % str(user_module))
    if user_module.col_num == 1:
        if hasattr(user_module, "data_proc"):
            data = user_module.data_proc(user_module.csv_path, user_module.col_num)
        else:
            data = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.python_test(data)
        end_time = time.time()

    elif user_module.col_num == 2:
        if hasattr(user_module, "data_proc"):
            data1, data2 = user_module.data_proc(user_module.csv_path)
        else:
            data1, data2 = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.python_test(data1, data2)
        end_time = time.time()

    elif user_module.col_num == 4:
        if hasattr(user_module, "data_proc"):
            x_min, y_min, x_max, y_max = user_module.data_proc(user_module.csv_path)
        else:
            x_min, y_min, x_max, y_max = data_proc(user_module.csv_path, user_module.col_num)
        begin_time = time.time()
        user_module.python_test(x_min, y_min, x_max, y_max)
        end_time = time.time()

    with open(output_file, "w") as out:
        out.writelines("run " + str(user_module) + " time is: " + str(end_time - begin_time) + "s")


def test_run(scheduler_file, output_path, args):
    with open(scheduler_file, "r") as f:
        for line in f:
            source_file = line.split(" ")[0]
            output_file = line.split(" ")[1]

            test_case = output_file.replace(output_file.split("/")[-1], "")
            out_python_path = output_path + "/python/" + test_case
            out_spark_path = output_path + "/spark/" + test_case
            if not os.path.exists(out_python_path):
                os.makedirs(out_python_path)
            if not os.path.exists(out_spark_path):
                os.makedirs(out_spark_path)

            user_module = importlib.import_module("test_case." + (source_file.split(".")[0]).replace("/", "."),
                                                  "test_case/" + source_file)

            if args.spark is not None:
                spark_test(out_spark_path + output_file.split("/")[-1], user_module, source_file)

            if args.python is not None:
                python_test(out_python_path + "/" + output_file.split("/")[-1], user_module)


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    parse.add_argument('-v --conda_env', dest='conda_env', nargs='*')
    parse.add_argument('-w --switch_env', dest='switch_env', nargs=1, default=True)
    parse.add_argument('-p --python', dest="python", nargs='*')
    parse.add_argument('-s --spark', dest="spark", nargs='*')

    args = parse.parse_args()

    if args.switch_env is not None:
        switch_env = eval(args.switch_env[0])
    else:
        switch_env = False

    if args.conda_env is not None:
        conda_env_file = args.conda_env[0]
    else:
        conda_env_file = "conf/arctern.yaml"

    if switch_env:
        switch_conda_environment(conda_env_file, args)
    else:
        if args.file is not None:
            scheduler_file = args.file[0]
        else:
            scheduler_file = "scheduler/gis_only/gis_test.txt"
        with open(conda_env_file, 'r') as env_f:
            yaml_conf = yaml.safe_load(env_f)
            dependencies = yaml_conf["dependencies"]
            version = dependencies[0].split("=")[1]
            commit_id = dependencies[0].split("=")[2].replace("*", "")
            output_path = "./output/" + version + "_" + commit_id
        test_run(scheduler_file, output_path, args)
    # Todo: read arctern version by web and write conda yaml
