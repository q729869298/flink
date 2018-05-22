#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(pwd)"/test-scripts/common.sh

#######################################
# Prints the given description, runs the given test and prints how long the execution took.
# Arguments:
#   $1: description of the test
#   $2: command to execute
#######################################
function run_test {
    description="$1"
    command="$2"

    printf "\n==============================================================================\n"
    printf "Running ${description}\n"
    printf "==============================================================================\n"
    start_timer
    ${command}
    exit_code="$?"
    end_timer

    check_logs_for_errors
    check_logs_for_exceptions
    check_logs_for_non_empty_out_files

    if [[ ${exit_code} == 0 ]]; then
        if [[ ! "$PASS" ]]; then
            echo "FAIL: Test exited with exit code 0 but the logs contained errors, exceptions or non-empty .out files"
            exit_code=1
        else
            echo "PASS: Test exited with exit code 0"
        fi
    else
        if [[ ! "$PASS" ]]; then
            echo "FAIL: Test exited with exit code ${exit_code} and the logs contained errors, exceptions or non-empty .out files"
        else
            echo "FAIL: Test exited with exit code ${exit_code}"
        fi
    fi

    cleanup

    if [[ ${exit_code} != 0 ]]; then
        exit "${exit_code}"
    fi
}

# Shuts down the cluster and cleans up all temporary folders and files. Make sure to clean up even in case of failures.
function cleanup {
  stop_cluster
  tm_kill_all
  jm_kill_all
  rm -rf $TEST_DATA_DIR 2> /dev/null
  revert_default_config
  rm -rf $FLINK_DIR/log/* 2> /dev/null
}

trap cleanup EXIT
