/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.operations.command;

import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.operations.ShowOperation;
import org.apache.flink.table.resource.ResourceUri;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW JARS statement. */
public class ShowJarsOperation implements ShowOperation {

    @Override
    public String asSummaryString() {
        return "SHOW JARS";
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        String[] jars =
                ctx.getResourceManager().getResources().keySet().stream()
                        .map(ResourceUri::getUri)
                        .toArray(String[]::new);
        return buildStringArrayResult("jars", jars);
    }
}
