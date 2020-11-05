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
from enum import Enum

from pyflink.java_gateway import get_gateway


class TimeDomain(Enum):
    """
    TimeDomain specifies whether a firing timer is based on event time or processing time.

    EVENT_TIME: Time is based on timestamp of events.
    PROCESSING_TIME: Time is based on the current processing-time of a machine where processing
                     happens.
    """

    EVENT_TIME = 0
    PROCESSING_TIME = 1
