#!/bin/bash

set -e

if [[ $# != 1 ]]; then
    echo 'usage: profile_toggle_all.sh enable|disable'
    exit 1
fi

ACTION=$1
if [[ $ACTION != "enable" && $ACTION != "disable" ]]; then
    echo 'Action must be either "enable" or "disable"'
    exit 1
fi

FILES=$(grep -n -R -l "PROFILE" src/)
python profile_toggle.py $ACTION $FILES
