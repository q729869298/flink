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

package org.apache.flink.configuration.description;

/**
 * Allows providing multiple formatters for the description. E.g. Html formatter, Markdown formatter etc.
 */
public abstract class Formatter {

	private StringBuilder state = new StringBuilder();

	/**
	 * Formats the description into a String using format specific tags.
	 *
	 * @param description description to be formatted
	 * @return string representation of the description
	 */
	public String format(Description description) {
		for (BlockElement blockElement : description.getBlocks()) {
			blockElement.format(this);
		}
		return finalizeFormatting();
	}

	public void format(LinkElement element) {
		formatLink(state, element.getLink(), element.getText());
	}

	public void format(TextElement element) {
		String[] inlineElements = element.getElements().stream().map(el -> {
				Formatter formatter = newInstance();
				el.format(formatter);
				return formatter.finalizeFormatting();
			}
		).toArray(String[]::new);
		formatText(state, element.getFormat(), inlineElements);
	}

	public void format(LineBreakElement element) {
		formatLineBreak(state);
	}

	public void format(ListElement element) {
		String[] inlineElements = element.getEntries().stream().map(el -> {
				Formatter formatter = newInstance();
				el.format(formatter);
				return formatter.finalizeFormatting();
			}
		).toArray(String[]::new);
		formatList(state, inlineElements);
	}

	private String finalizeFormatting() {
		String result = state.toString();
		state.setLength(0);
		return result;
	}

	protected abstract void formatLink(StringBuilder state, String link, String description);

	protected abstract void formatLineBreak(StringBuilder state);

	protected abstract void formatText(StringBuilder state, String format, String[] elements);

	protected abstract void formatList(StringBuilder state, String[] entries);

	protected abstract Formatter newInstance();

}

