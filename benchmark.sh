#!/bin/bash

conda_env_file="conf/arctern.conf"

# shellcheck disable=SC2013
# shellcheck disable=SC2006
for commit_info in `cat $conda_env_file`
do
  echo "$commit_info"
  conda_env_name=${commit_info//"*"/""}
#  echo ${conda_env_name//"="/"-"}
  python3 conf/create_conda_env.py -v ${conda_env_name//"="/"-"}
  conda activate ${conda_env_name//"="/"-"}
  python3 main.py
done
