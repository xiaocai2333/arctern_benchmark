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
import subprocess

from conda_conf import create_conda_env, get_conda_prefix
from python_benchmark import python_test


def reboot(conda_prefix, argv):
    print(conda_prefix)
    exec_python_path = conda_prefix + "/bin/python"
    for n, e in enumerate(argv):
        if e == "-w":
            argv[n + 1] = "False"
    os.execlp(exec_python_path, "arctern test", *argv)


def conf_spark_env():
    import re
    spark_submit_path = subprocess.check_output("which spark-submit", shell=True).decode('utf-8').replace("\n", "")
    spark_conf_path = os.path.join("/".join(spark_submit_path.split("/")[0:-1]), "../conf")
    conf_env_path = sys.prefix
    spark_env_f = open(spark_conf_path + "/spark-env.sh", "r+")
    source_env_text = spark_env_f.readlines()
    pyspark_path_exist = False
    for i in range(len(source_env_text)):
        if re.search("PYSPARK_PYTHON", source_env_text[i]):
            source_env_text[i] = "export PYSPARK_PYTHON=%s/bin/python" % conf_env_path
            pyspark_path_exist = True
    if not pyspark_path_exist:
        source_env_text.append("export PYSPARK_PYTHON=%s/bin/python" % conf_env_path)
    target_env_file = open(spark_conf_path + "/spark-env.sh", "w+")
    target_env_file.writelines(source_env_text)
    target_env_file.close()

    spark_default_f = open(spark_conf_path + "/spark-defaults.conf", "r+")
    source_default_text = spark_default_f.readlines()
    proj_exist = False
    gdal_exist = False
    for i in range(len(source_default_text)):
        if re.search("spark.executorEnv.PROJ_LIB", source_default_text[i]):
            source_default_text[i] = "spark.executorEnv.PROJ_LIB %s/share/proj" % conf_env_path
            proj_exist = True
        if re.search("spark.executorEnv.PROJ_LIB", source_default_text[i]):
            source_default_text[i] = "spark.executorEnv.GDAL_DATA %s/share/gdal" % conf_env_path
            gdal_exist = True
    if not proj_exist:
        source_default_text.append("spark.executorEnv.PROJ_LIB %s/share/proj" % conf_env_path)
    if not gdal_exist:
        source_default_text.append("spark.executorEnv.GDAL_DATA %s/share/gdal" % conf_env_path)
    target_default__file = open(spark_conf_path + "/spark-defaults.conf", "w+")
    target_default__file.writelines(source_default_text)
    target_default__file.close()


def spark_test(source_file, output_file, run_times, commit_id):
    conf_spark_env()
    command = "spark-submit ./spark_benchmark.py -s %s -o %s -t %s -v %s" % (
        source_file, output_file, run_times, commit_id)
    os.system(command)


def run_test(scheduler_file, version, commit_id, test_spark, test_python):
    version_commit_id = version + "-" + commit_id
    output_path = "output/" + version_commit_id
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
                spark_test(source_file, out_spark_path + output_file.split("/")[-1].replace("\n", ""),
                           run_time, version_commit_id)

            if test_python:
                python_test(out_python_path + "/" + output_file.split("/")[-1].replace("\n", ""),
                                             user_module, run_time, version_commit_id)


def parse_args():
    parse = argparse.ArgumentParser()
    parse.add_argument('-f --file', dest='file', nargs=1, default=None)
    parse.add_argument('--python', dest="python", nargs='*')
    parse.add_argument('--spark', dest="spark", nargs='*')
    parse.add_argument('-w --switch_env', dest='switch_env', nargs=1, default=False)
    parse.add_argument('--times', dest='times', nargs=1)
    parse.add_argument('-v --version', dest='version', nargs=1)
    parse.add_argument('--commit_id', dest='commit_id', nargs=1)
    parse.add_argument('--gpu', dest='gpu', nargs='*')
    args = parse.parse_args()

    if args.switch_env is not None:
        args.switch_env = eval(args.switch_env[0])
    else:
        args.switch_env = False

    return args


if __name__ == "__main__":
    args = parse_args()
    run_time = eval(args.times[0])

    version = args.version[0]
    commit_id = args.commit_id[0]
    test_spark = False
    test_python = False
    if args.python is not None:
        test_python = True
    if args.spark is not None:
        test_spark = True
    if args.switch_env:
        status = create_conda_env(version, commit_id=commit_id, is_spark=test_spark, is_gpu=False,
                                  channel="arctern-dev", conda_label="")
        if status in [0, 256]:
            conda_prefix = get_conda_prefix(version + "-" + commit_id)
            reboot(conda_prefix, sys.argv)
        else:
            print("conda env create failed!")
    else:
        if args.file is not None:
            scheduler_file = args.file[0]
        else:
            scheduler_file = "scheduler/gis_only/gis_test.txt"

        run_test(scheduler_file, version, commit_id, test_spark, test_python)
