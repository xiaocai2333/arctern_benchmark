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

import os, subprocess

__all__ = [
    "create_conda_env",
    "get_conda_prefix",
]


def create_conda_env(version, commit_id="", is_spark=False, is_gpu=False, channel="arctern-dev", conda_label=""):
    if not conda_label:
        conda_label = "label/cuda10.0" if is_gpu else ""

    def get_libarctern_label():
        return f"{channel}/{conda_label}::" if conda_label else ""

    def get_version():
        return f"={version}" if version else ""

    def get_commit_id():
        return f"={commit_id}*" if commit_id else ""

    def get_libarctern_package():
        return f"{get_libarctern_label()}libarctern{get_version()}{get_commit_id()}"

    def get_arctern_package():
        return f"arctern{get_version()}{get_commit_id()}"

    def get_arcternspark_package():
        return f"arctern-spark{get_version()}{get_commit_id()}" if is_spark else ""

    conda_env_name = (version + "-" + commit_id)

    cmd = (
        f"conda create -n {conda_env_name} "
        f"-c conda-forge "
        f"-c {channel} "
        f"-y "
        f"{get_libarctern_package()} "
        f"{get_arctern_package()} "
        f"{get_arcternspark_package()} "
    )
    status = os.system(cmd)
    return status


def get_conda_prefix(conda_env_name):
    conda_prefix = subprocess.check_output("conda env list | grep %s" % conda_env_name, shell=True).decode(
        'utf-8').split(" ")[-1].replace("\n", "")
    return conda_prefix
