/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.test.wordcount;

import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.StringValue;

public class WordCountDummySource extends UserSourceInvokable {

	private StringValue lineValue = new StringValue("");
	StreamRecord record = new StreamRecord(lineValue);

	public WordCountDummySource() {
	}

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 10000; i++) {
			if (i % 2 == 0) {
				lineValue.setValue("Gyula Marci");
			} else {
				lineValue.setValue("Gabor Gyula");
			}
			record.setRecord(lineValue);
			emit(record);
		}
	}
}