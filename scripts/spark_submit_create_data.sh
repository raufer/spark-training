#!/bin/bash

SCRIPT_PATH=$(readlink -f $0)
CWD=$(dir_name $SCRIPT_PATH)

config_file=config.json
json_config_file_path=$CWD/../app/data/resources/$config_file

export PYTHONPATH="$CWD/..:${PYTHONPATH}"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --files $json_config_file_path \
    $CWD/../app/__init__.py -j $json_config_file_path -a create
