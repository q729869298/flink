/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package org.apache.flink.api.java.functions;

import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;

public class FormattingMapper<T> extends MapFunction<T, String> {
	private TextFormatter formatter;

	public FormattingMapper(TextOutputFormat.TextFormatter formatter) {
		this.formatter = formatter;
	}

	@Override
	public String map(T value) throws Exception {
		return formatter.format(value);
	}
}
