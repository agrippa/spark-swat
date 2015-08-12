#!/bin/bash

source ~/.bash_profile
setup_nodes

teardown_all.sh
force_kill.sh

setup_all.sh
