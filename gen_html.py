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
import os
from collect_result import gen_data_path

REP_KEYS_MAP = {
    "perf": ('ROWS', 'REP_SET_NAMES', 'REP_DATASETS', 'REP_FUNC_NAMES'),
    "scale": ('REP_NODES', 'REP_SET_NAMES', 'REP_DATASETS', 'REP_FUNC_NAMES'),
}


def read_and_replace(data_path, mode, template_path, output_path):

    with open(data_path, "r") as f:
        lines = f.readlines()
        string_data = "".join(lines)
    string_data = string_data or "{}"
    import ast
    eval = ast.literal_eval
    rep_data = eval(string_data)
    assert rep_data

    rep_keys = REP_KEYS_MAP[mode]

    with open(template_path, "r") as f:
        lines = f.readlines()
        all_string = "".join(lines)
        for k in rep_keys:
            v = rep_data[k]
            all_string = all_string.replace(k, str(v))

    if all_string:
        with open(output_path, "w") as f:
            f.write(all_string)


def gen_html():
    mode = "scale"
    template_path = "result_html/perf_scale.template"
    data_path = "result_html/data_path"
    output_path = "result_html/"
    for file in os.listdir(data_path):
        data_file = os.path.join(data_path, file)
        output_file = os.path.join(output_path, file.replace(".txt", ".html"))
        read_and_replace(data_file, mode, template_path, output_file)


def parse_args():
    parse = argparse.ArgumentParser()
    parse.add_argument('--python', dest="python", nargs='*')
    parse.add_argument('--spark', dest="spark", nargs='*')
    args = parse.parse_args()

    return args


if __name__ == "__main__":
    args = parse_args()
    test_list = []
    if args.spark is not None:
        test_list.append("spark")
    if args.python is not None:
        test_list.append("python")
    gen_data_path(test_list)
    gen_html()



