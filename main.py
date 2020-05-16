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
import importlib
import os
import sys
import yaml
from python import python_benchmark
from gen_html import collect_result, gen_html


def switch_conda_environment(conda_environment_file):
    with open(conda_environment_file, "r") as env_f:
        commit_info = env_f.readline()
    version = commit_info.split("=")[0]
    commit_id = commit_info.split("=")[-1].replace("\n", "")
    conda_env_name = (version + "-" + commit_id).replace("*", "")
    conda_env_dict = {"name": conda_env_name, "channels": ["conda-forge", "arctern-dev"],
                      "dependencies": ["libarctern=" + version + "=" + commit_id,
                                       "arctern=" + version + "=" + commit_id,
                                       "arctern-spark=" + version + "=" + commit_id,
                                       "pyyaml"]}
    with open("conf/arctern.yaml", "w") as conda_env_f:
        yaml_obj = yaml.dump(conda_env_dict)
        conda_env_f.write(yaml_obj)

    status = os.system("conda env create -f conf/arctern.yaml")

    if status >= 0:
        original_conda_file = open("conf/arctern_version.conf", "r")
        original_conda_env_list = original_conda_file.readlines()
        delete_current_conda_env_file = open("conf/arctern_version.conf", "w")
        delete_current_conda_env_list = "".join(original_conda_env_list[1:])
        delete_current_conda_env_file.write(delete_current_conda_env_list)
        original_conda_file.close()
        delete_current_conda_env_file.close()
        conda_prefix = os.popen("conda env list | grep %s" % conda_env_name).read().split(" ")[-1].replace(
            "\n", "")
        exec_python_path = conda_prefix + "/bin/python"
        for n, e in enumerate(sys.argv):
            if e == "-w":
                sys.argv[n + 1] = "False"
            if e == "-c":
                sys.argv[n+1] = "False"
        print(exec_python_path)
        os.execlp(exec_python_path, "arctern test", *sys.argv)
    else:
        print("create conda environment failed!")
        sys.exit(1)


def spark_test(output_file, source_file, run_times, commit_id):
    command = "spark-submit ./spark/spark_benchmark.py -s %s -o %s -t %s -v %s" % (
        source_file, output_file, run_times, commit_id)
    print(command)
    os.system(command)


def run_test(scheduler_file, commit_id, test_spark, test_python):

    output_path = "output/" + commit_id

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

            if test_spark:
                spark_test(out_spark_path + output_file.split("/")[-1].replace("\n", ""), source_file, run_time, commit_id)

            if test_python:
                python_benchmark.python_test(out_python_path + "/" + output_file.split("/")[-1].replace("\n", ""),
                                             user_module, run_time, commit_id)


def tag_commit_build_time():
    import arctern
    import re
    version_info = arctern.version().split("\n")
    print(version_info)
    build_time = ""
    commit_id = sys.prefix.replace("\n", "").split("/")[-1]
    for info in version_info:
        if re.search("build time", info):
            build_time = info.replace("build time : ", "")

    version_build_time = {"build_time": build_time,
                          "commit_id": commit_id}

    with open("gen_html/version_build_time.txt", "a+") as file:
        file.writelines(str(version_build_time) + "\n")

    return commit_id


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    parse.add_argument('-v --conda_env', dest='conda_env', nargs='*')
    parse.add_argument('-w --switch_env', dest='switch_env', nargs=1, default=True)
    parse.add_argument('-p --python', dest="python", nargs='*')
    parse.add_argument('-s --spark', dest="spark", nargs='*')
    parse.add_argument('-t --time', dest='time', nargs=1)
    parse.add_argument('-c --copy_conf', dest='copy_conf', nargs=1)

    args = parse.parse_args()

    if args.switch_env is not None:
        switch_env = eval(args.switch_env[0])
    else:
        switch_env = False

    conda_env_file = "conf/arctern_version.conf"
    print(sys.prefix)
    run_time = eval(args.time[0])
    if eval(args.copy_conf[0]):
        os.system("cp conf/arctern.conf %s" % conda_env_file)
        os.system("rm gen_html/version_build_time.txt")

    if switch_env:
        switch_conda_environment(conda_env_file)
    else:
        if args.file is not None:
            scheduler_file = args.file[0]
        else:
            scheduler_file = "scheduler/gis_only/gis_test.txt"
        test_spark = False
        test_python = False
        if args.python is not None:
            test_python = True
        if args.spark is not None:
            test_spark = True

        commit_id = tag_commit_build_time()

        run_test(scheduler_file, commit_id, test_spark, test_python)

        with open(conda_env_file, "r") as f:
            if f.readline() != "":
                switch_conda_environment(conda_env_file)

        test_list = []
        if test_python:
            test_list.append("python")
        if test_spark:
            test_list.append("spark")
        collect_result.gen_data_path(test_list)
        gen_html.gen_html()
