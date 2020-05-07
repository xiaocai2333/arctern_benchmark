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


import subprocess
import yaml


if __name__ == "__main__":

    config_file = "./conf/spark.yaml"

    with open(config_file, "r") as f:
        conf = yaml.load(f)

    # hadoop = conf["hadoop"]
    master = conf["spark"]["master"]
    deploy = conf["spark"]["deploy-module"]
    executor_memory = conf["spark"]["executor-memory"]
    hadoop = None
    if hadoop is not None:
        shell = "spark-submit --master " + master + " --deploy-module " + deploy + \
                " --executor-memory " + executor_memory + " arctern_benchmark.py"
    else:
        shell = "spark-submit arctern_benchmark.py -s gis_only/gis_test.txt "
    subprocess.Popen(shell, shell=True)

