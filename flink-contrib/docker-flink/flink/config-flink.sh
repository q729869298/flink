#!/bin/bash

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

#set nb_slots = nb CPUs
#let "nbslots=$2 *`nproc"
sed -i -e "s/%nb_slots%/`nproc`/g" $FLINK_HOME/conf/flink-conf.yaml
#set parallelism
sed -i -e "s/%parallelism%/1/g" $FLINK_HOME/conf/flink-conf.yaml

if [ "$1" = "jobmanager" ]; then
    echo "Configuring Job Manager on this node"
    sed -i -e "s/%jobmanager%/`hostname -i`/g" $FLINK_HOME/conf/flink-conf.yaml
    $FLINK_HOME/bin/jobmanager.sh start cluster

elif [ "$1" = "taskmanager" ]; then
    echo "Configuring Task Manager on this node"
    sed -i -e "s/%jobmanager%/$JOBMANAGER_PORT_6123_TCP_ADDR/g" $FLINK_HOME/conf/flink-conf.yaml
    $FLINK_HOME/bin/taskmanager.sh start
fi

#print out config - debug
echo "config file: " && cat $FLINK_HOME/conf/flink-conf.yaml

#Uncomment for SSH connection between nodes without prompts
#echo 'export FLINK_SSH_OPTS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"' >> ~/.profile

#run ssh server and supervisor to keep container running.
/usr/sbin/sshd && supervisord -c /etc/supervisor/supervisor.conf
