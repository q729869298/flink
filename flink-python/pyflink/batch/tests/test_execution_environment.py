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
import json
import os
import tempfile

from py4j.protocol import Py4JJavaError

from pyflink.batch import ExecutionEnvironment
from pyflink.table import DataTypes, BatchTableEnvironment, CsvTableSource, CsvTableSink
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ExecutionEnvironmentTests(PyFlinkTestCase):

    def setUp(self):
        self.env = ExecutionEnvironment.get_execution_environment()

    def test_get_set_parallelism(self):

        self.env.set_parallelism(10)

        parallelism = self.env.get_parallelism()

        assert parallelism == 10

    def test_get_session_timeout(self):

        timeout = self.env.get_session_timeout()

        assert timeout == 0

    def test_set_session_timeout(self):

        self.assertRaisesRegexp(Py4JJavaError,
                                "Support for sessions is currently disabled\\. "
                                "It will be enabled in future Flink versions\\.",
                                self.env.set_session_timeout, 1000)

    def test_get_set_default_local_parallelism(self):

        self.env.set_default_local_parallelism(8)

        parallelism = self.env.get_default_local_parallelism()

        assert parallelism == 8

    def test_add_default_kryo_serializer(self):

        self.env.add_default_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

    def test_register_type_with_kryo_serializer(self):

        self.env.register_type_with_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

    def test_register_type(self):

        self.env.register_type("org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

    def test_get_execution_plan(self):
        tmp_dir = tempfile.gettempdir()
        source_path = os.path.join(tmp_dir + '/streaming.csv')
        tmp_csv = os.path.join(tmp_dir + '/streaming2.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        t_env = BatchTableEnvironment.create(self.env)
        csv_source = CsvTableSource(source_path, field_names, field_types)
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Results",
            field_names, field_types, CsvTableSink(tmp_csv))
        t_env.scan("Orders").insert_into("Results")

        plan = t_env.exec_env().get_execution_plan()

        json.loads(plan)
