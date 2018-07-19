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

source "$(dirname "$0")"/common.sh

DOCKER_MODULE_DIR=${END_TO_END_DIR}/../flink-container/docker
DOCKER_SCRIPTS=${END_TO_END_DIR}/test-scripts/container-scripts

export FLINK_JOB=org.apache.flink.examples.java.wordcount.WordCount
export FLINK_DOCKER_IMAGE_NAME=test_docker_embedded_job
export INPUT_VOLUME=${END_TO_END_DIR}/test-scripts/test-data
export OUTPUT_VOLUME=${TEST_DATA_DIR}/out
export INPUT_PATH=/data/test/input
export OUTPUT_PATH=/data/test/output
export FLINK_JOB_ARGUMENTS="--input ${INPUT_PATH}/words --output ${OUTPUT_PATH}/docker_wc_out"

# user inside the container must be able to createto workaround in-container permissions
mkdir -p $OUTPUT_VOLUME
chmod 777 $OUTPUT_VOLUME

cd "$DOCKER_MODULE_DIR"
./build.sh --from-local-dist --job-jar ${FLINK_DIR}/examples/batch/WordCount.jar --image-name ${FLINK_DOCKER_IMAGE_NAME}
cd "$END_TO_END_DIR"

docker-compose -f ${DOCKER_MODULE_DIR}/docker-compose.yml -f ${DOCKER_SCRIPTS}/docker-compose.test.yml up --abort-on-container-exit --exit-code-from job-cluster &> /dev/null
docker-compose -f ${DOCKER_MODULE_DIR}/docker-compose.yml -f ${DOCKER_SCRIPTS}/docker-compose.test.yml logs job-cluster > ${FLINK_DIR}/log/jobmanager.log
docker-compose -f ${DOCKER_MODULE_DIR}/docker-compose.yml -f ${DOCKER_SCRIPTS}/docker-compose.test.yml logs taskmanager > ${FLINK_DIR}/log/taskmanager.log

check_result_hash "WordCount" $OUTPUT_VOLUME/docker_wc_out "72a690412be8928ba239c2da967328a5"
