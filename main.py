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


def spark_test(output_file, source_file, run_times, commit_id, version):
    command = "spark-submit ./spark/spark_benchmark.py -s %s -o %s -t %s -c %s -v %s" % (
        source_file, output_file.replace("\n", ""), run_times, commit_id, version)
    print(command)
    os.system(command)


def run_test(scheduler_file, output_path, args, run_time, commit_id, version):
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
                spark_test(out_spark_path + output_file.split("/")[-1], source_file, run_time, commit_id, version)

            if args.python is not None:
                python_benchmark.python_test(out_python_path + "/" + output_file.split("/")[-1], user_module, run_time,
                                             commit_id, version)


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    parse.add_argument('-v --conda_env', dest='conda_env', nargs='*')
    parse.add_argument('-w --switch_env', dest='switch_env', nargs=1, default=True)
    parse.add_argument('-p --python', dest="python", nargs='*')
    parse.add_argument('-s --spark', dest="spark", nargs='*')
    parse.add_argument('-t --time', dest='time', nargs=1)

    args = parse.parse_args()

    if args.switch_env is not None:
        switch_env = eval(args.switch_env[0])
    else:
        switch_env = False

    if args.conda_env is not None:
        conda_env_file = args.conda_env[0]
    else:
        conda_env_file = "conf/arctern.yaml"

    run_time = int(args.time[0])
    print(run_time)
    # Todo: create multiple conda env
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
            output_path = "./output/" + version + "/" + commit_id
        run_test(scheduler_file, output_path, args, run_time, commit_id, version)

        with open(scheduler_file, "r") as f:
            line = f.readline()
            test_case_path = line.split(" ")[0].replace(line.split(" ")[0].split("/")[-1], "")
        collect_result.pref_data(test_case_path)
        gen_html.gen_html()
    # Todo: draw web map
