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
import yaml


def create_conda_environment(version_info):

    version_info = version_info.replace("\n", "")
    conda_env_name = version_info.replace("=", "-").replace("*", "")
    conda_env_dict = {"name": conda_env_name, "channels": ["conda-forge", "arctern-dev"],
                      "dependencies": ["libarctern=" + version_info,
                                       "arctern=" + version_info,
                                       "arctern-spark=" + version_info,
                                       "pyyaml"]}
    with open("conf/arctern.yaml", "w") as conda_env_f:
        yaml_obj = yaml.dump(conda_env_dict)
        conda_env_f.write(yaml_obj)


if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('-v --conda_env', dest='conda_env', nargs=1)

    args = parse.parse_args()

    version_commit_id = args.conda_env[0]

    create_conda_environment(version_commit_id)
