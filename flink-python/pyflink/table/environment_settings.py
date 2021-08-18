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
import warnings
from pyflink.java_gateway import get_gateway

from pyflink.common import Configuration

__all__ = ['EnvironmentSettings']


class EnvironmentSettings(object):
    """
    Defines all parameters that initialize a table environment. Those parameters are used only
    during instantiation of a :class:`~pyflink.table.TableEnvironment` and cannot be changed
    afterwards.

    Example:
    ::

        >>> EnvironmentSettings.new_instance() \\
        ...     .in_streaming_mode() \\
        ...     .with_built_in_catalog_name("my_catalog") \\
        ...     .with_built_in_database_name("my_database") \\
        ...     .build()

    :func:`EnvironmentSettings.in_streaming_mode` or :func:`EnvironmentSettings.in_batch_mode`
    might be convenient as shortcuts.
    """

    class Builder(object):
        """
        A builder for :class:`EnvironmentSettings`.
        """

        def __init__(self):
            gateway = get_gateway()
            self._j_builder = gateway.jvm.EnvironmentSettings.Builder()

        def use_old_planner(self) -> 'EnvironmentSettings.Builder':
            """
            .. note:: The old planner has been removed in Flink 1.14. Since there is only one
                      planner left (previously called the 'blink' planner), this setting will
                      throw an exception.
            """
            warnings.warn(
                "Deprecated in 1.13. Please update to the new planner (i.e. Blink planner).",
                DeprecationWarning)
            self._j_builder = self._j_builder.useOldPlanner()
            return self

        def use_blink_planner(self) -> 'EnvironmentSettings.Builder':
            """
            Sets the Blink planner as the required module.

            This is the default behavior.

            .. note:: The old planner has been removed in Flink 1.14. Since there is only one
                      planner left (previously called the 'blink' planner), this setting is
                      obsolete and will be removed in future versions.

            :return: This object.
            """
            warnings.warn(
                "Deprecated in 1.14. A planner declaration is not required anymore.",
                DeprecationWarning)
            self._j_builder = self._j_builder.useBlinkPlanner()
            return self

        def use_any_planner(self) -> 'EnvironmentSettings.Builder':
            """
            Does not set a planner requirement explicitly.

            A planner will be discovered automatically, if there is only one planner available.

            By default, :func:`use_blink_planner` is enabled.

            .. note:: The old planner has been removed in Flink 1.14. Since there is only one
                      planner left (previously called the 'blink' planner), this setting is
                      obsolete and will be removed in future versions.

            :return: This object.
            """
            warnings.warn(
                "Deprecated in 1.14. A planner declaration is not required anymore.",
                DeprecationWarning)
            self._j_builder = self._j_builder.useAnyPlanner()
            return self

        def in_batch_mode(self) -> 'EnvironmentSettings.Builder':
            """
            Sets that the components should work in a batch mode. Streaming mode by default.

            :return: This object.
            """
            self._j_builder = self._j_builder.inBatchMode()
            return self

        def in_streaming_mode(self) -> 'EnvironmentSettings.Builder':
            """
            Sets that the components should work in a streaming mode. Enabled by default.

            :return: This object.
            """
            self._j_builder = self._j_builder.inStreamingMode()
            return self

        def with_built_in_catalog_name(self, built_in_catalog_name: str) \
                -> 'EnvironmentSettings.Builder':
            """
            Specifies the name of the initial catalog to be created when instantiating
            a :class:`~pyflink.table.TableEnvironment`.

            This catalog is an in-memory catalog that will be used to store all temporary objects
            (e.g. from :func:`~pyflink.table.TableEnvironment.create_temporary_view` or
            :func:`~pyflink.table.TableEnvironment.create_temporary_system_function`) that cannot
            be persisted because they have no serializable representation.

            It will also be the initial value for the current catalog which can be altered via
            :func:`~pyflink.table.TableEnvironment.use_catalog`.

            Default: "default_catalog".

            :param built_in_catalog_name: The specified built-in catalog name.
            :return: This object.
            """
            self._j_builder = self._j_builder.withBuiltInCatalogName(built_in_catalog_name)
            return self

        def with_built_in_database_name(self, built_in_database_name: str) \
                -> 'EnvironmentSettings.Builder':
            """
            Specifies the name of the default database in the initial catalog to be
            created when instantiating a :class:`~pyflink.table.TableEnvironment`.

            This database is an in-memory database that will be used to store all temporary
            objects (e.g. from :func:`~pyflink.table.TableEnvironment.create_temporary_view` or
            :func:`~pyflink.table.TableEnvironment.create_temporary_system_function`) that cannot
            be persisted because they have no serializable representation.

            It will also be the initial value for the current catalog which can be altered via
            :func:`~pyflink.table.TableEnvironment.use_catalog`.

            Default: "default_database".

            :param built_in_database_name: The specified built-in database name.
            :return: This object.
            """
            self._j_builder = self._j_builder.withBuiltInDatabaseName(built_in_database_name)
            return self

        def build(self) -> 'EnvironmentSettings':
            """
            Returns an immutable instance of EnvironmentSettings.

            :return: an immutable instance of EnvironmentSettings.
            """
            return EnvironmentSettings(self._j_builder.build())

    def __init__(self, j_environment_settings):
        self._j_environment_settings = j_environment_settings

    def get_built_in_catalog_name(self) -> str:
        """
        Gets the specified name of the initial catalog to be created when instantiating a
        :class:`~pyflink.table.TableEnvironment`.

        :return: The specified name of the initial catalog to be created.
        """
        return self._j_environment_settings.getBuiltInCatalogName()

    def get_built_in_database_name(self) -> str:
        """
        Gets the specified name of the default database in the initial catalog to be created when
        instantiating a :class:`~pyflink.table.TableEnvironment`.

        :return: The specified name of the default database in the initial catalog to be created.
        """
        return self._j_environment_settings.getBuiltInDatabaseName()

    def is_blink_planner(self) -> bool:
        """
        Tells if :class:`~pyflink.table.TableEnvironment` should work in a blink or old
        planner.

        .. note:: The old planner has been removed in Flink 1.14. Since there is only one
                  planner left (previously called the 'blink' planner), this method is
                  obsolete and will be removed in future versions.

        :return: True if the TableEnvironment should work in a blink planner, false otherwise.
        """
        warnings.warn(
            "Deprecated in 1.14. There is only one planner anymore.",
            DeprecationWarning)
        return self._j_environment_settings.isBlinkPlanner()

    def is_streaming_mode(self) -> bool:
        """
        Tells if the :class:`~pyflink.table.TableEnvironment` should work in a batch or streaming
        mode.

        :return: True if the TableEnvironment should work in a streaming mode, false otherwise.
        """
        return self._j_environment_settings.isStreamingMode()

    def to_configuration(self) -> Configuration:
        """
        Convert to `pyflink.common.Configuration`.

        It sets the `table.planner` and `execution.runtime-mode` according to the current
        EnvironmentSetting.

        :return: Configuration with specified value.
        """
        return Configuration(j_configuration=self._j_environment_settings.toConfiguration())

    @staticmethod
    def new_instance() -> 'EnvironmentSettings.Builder':
        """
        Creates a builder for creating an instance of EnvironmentSettings.

        By default, it does not specify a required planner and will use the one that is available
        on the classpath via discovery.

        :return: A builder of EnvironmentSettings.
        """
        return EnvironmentSettings.Builder()

    @staticmethod
    def from_configuration(config: Configuration) -> 'EnvironmentSettings':
        """
        Creates the EnvironmentSetting with specified Configuration.

        :return: EnvironmentSettings.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.fromConfiguration(config._j_configuration))

    @staticmethod
    def in_streaming_mode() -> 'EnvironmentSettings':
        """
        Creates a default instance of EnvironmentSettings in streaming execution mode.

        In this mode, both bounded and unbounded data streams can be processed.

        This method is a shortcut for creating a :class:`~pyflink.table.TableEnvironment` with
        little code. Use the builder provided in :func:`EnvironmentSettings.new_instance` for
        advanced settings.

        :return: EnvironmentSettings.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.inStreamingMode())

    @staticmethod
    def in_batch_mode() -> 'EnvironmentSettings':
        """
        Creates a default instance of EnvironmentSettings in batch execution mode.

        This mode is highly optimized for batch scenarios. Only bounded data streams can be
        processed in this mode.

        This method is a shortcut for creating a :class:`~pyflink.table.TableEnvironment` with
        little code. Use the builder provided in :func:`EnvironmentSettings.new_instance` for
        advanced settings.

        :return: EnvironmentSettings.
        """
        return EnvironmentSettings(
            get_gateway().jvm.EnvironmentSettings.inBatchMode())
