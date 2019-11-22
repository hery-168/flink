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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.metrics.CharacterFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents the format after which the "scope" (or namespace) of the various
 * component metric groups is built. Component metric groups are for example
 * "TaskManager", "Task", or "Operator".
 *
 * <p>User defined scope formats allow users to include or exclude
 * certain identifiers from the scope. The scope for metrics belonging to the "Task"
 * group could for example include the task attempt number (more fine grained identification), or
 * exclude it (continuity of the namespace across failure and recovery).
 */
public abstract class ScopeFormat {

	private static CharacterFilter defaultFilter = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input;
		}
	};

	// ------------------------------------------------------------------------
	//  Scope Format Special Characters
	// ------------------------------------------------------------------------

	/**
	 * If the scope format starts with this character, then the parent components scope
	 * format will be used as a prefix.
	 *
	 * <p>For example, if the TaskManager's job format is {@code "*.<job_name>"}, and the
	 * TaskManager format is {@code "<host>"}, then the job's metrics
	 * will have {@code "<host>.<job_name>"} as their scope.
	 */
	public static final String SCOPE_INHERIT_PARENT = "*";

	public static final String SCOPE_SEPARATOR = ".";

	private static final String SCOPE_VARIABLE_PREFIX = "<";
	private static final String SCOPE_VARIABLE_SUFFIX = ">";

	// ------------------------------------------------------------------------
	//  Scope Variables
	// ------------------------------------------------------------------------

	public static final String SCOPE_HOST = asVariable("host");

	// ----- Task Manager ----

	public static final String SCOPE_TASKMANAGER_ID = asVariable("tm_id");

	// ----- Job -----

	public static final String SCOPE_JOB_ID = asVariable("job_id");
	public static final String SCOPE_JOB_NAME = asVariable("job_name");

	// ----- Task ----

	public static final String SCOPE_TASK_VERTEX_ID = asVariable("task_id");
	public static final String SCOPE_TASK_NAME = asVariable("task_name");
	public static final String SCOPE_TASK_ATTEMPT_ID = asVariable("task_attempt_id");
	public static final String SCOPE_TASK_ATTEMPT_NUM = asVariable("task_attempt_num");
	public static final String SCOPE_TASK_SUBTASK_INDEX = asVariable("subtask_index");

	// ----- Operator ----

	public static final String SCOPE_OPERATOR_ID = asVariable("operator_id");
	public static final String SCOPE_OPERATOR_NAME = asVariable("operator_name");


	// ------------------------------------------------------------------------
	//  Scope Format Base
	// ------------------------------------------------------------------------

	/** The scope format. */
	// 原始格式
	private final String format;

	/** The format, split into components. */
	//format按照分割符分割后的数组
	private final String[] template;
  // 这是template数组中，变量元素的索引
	private final int[] templatePos;

	private final int[] valuePos;

	// ------------------------------------------------------------------------
 //假设 入参值为 format="<host>.jobmanager" ，parent=null ， variables={"<host，job_id，job_name>"}
	protected ScopeFormat(String format, ScopeFormat parent, String[] variables) {
		checkNotNull(format, "format is null");
		//将format这个字符串分割， rawComponents = {"<host>", "jobmanager"}
		final String[] rawComponents = format.split("\\" + SCOPE_SEPARATOR);

		// compute the template array
		//根据rawComponents的第一个元素是为"*"，来判断是否要继承父组的范围
		final boolean parentAsPrefix = rawComponents.length > 0 && rawComponents[0].equals(SCOPE_INHERIT_PARENT);
		if (parentAsPrefix) {
			//需要继承父组的范围，而父组有是null，则抛出异常
			if (parent == null) {
				throw new IllegalArgumentException("Component scope format requires parent prefix (starts with '"
					+ SCOPE_INHERIT_PARENT + "'), but this component has no parent (is root component).");
			}
			//如果以 "*." 开头，则format至少需要有3个字符，否则就是无效字符，设置为 "<empty>"
			this.format = format.length() > 2 ? format.substring(2) : "<empty>";

			String[] parentTemplate = parent.template;
			int parentLen = parentTemplate.length;
			//将父组的范围和自身的范围，合并到一起
			this.template = new String[parentLen + rawComponents.length - 1];
			System.arraycopy(parentTemplate, 0, this.template, 0, parentLen);
			System.arraycopy(rawComponents, 1, this.template, parentLen, rawComponents.length - 1);
		}
		else {
			//不需要继承父组的范围，则直接赋值，format="<host>.jobmanager"，template={"<host>", "jobmanager"}
			this.format = format.isEmpty() ? "<empty>" : format;
			this.template = rawComponents;
		}

		// --- compute the replacement matrix ---
		// a bit of clumsy Java collections code ;-)
		// 将 variables={"<host>"} 转换为map {"<host>" -> 0}
		HashMap<String, Integer> varToValuePos = arrayToMap(variables);
		List<Integer> templatePos = new ArrayList<>();
		List<Integer> valuePos = new ArrayList<>();

		for (int i = 0; i < template.length; i++) {
			final String component = template[i];

			// check if that is a variable
			//检查当前这个组件是否是一个变量
			if (component != null && component.length() >= 3 &&
					component.charAt(0) == '<' && component.charAt(component.length() - 1) == '>') {

				// this is a variable
				//这是一个变量，则从上面的map中，获取其索引
				Integer replacementPos = varToValuePos.get(component);
				if (replacementPos != null) {
					templatePos.add(i);
					valuePos.add(replacementPos);
				}
			}
		}

		this.templatePos = integerListToArray(templatePos);
		this.valuePos = integerListToArray(valuePos);
	}

	// ------------------------------------------------------------------------

	public String format() {
		return format;
	}

	protected final String[] copyTemplate() {
		String[] copy = new String[template.length];
		System.arraycopy(template, 0, copy, 0, template.length);
		return copy;
	}

	protected final String[] bindVariables(String[] template, String[] values) {
		final int len = templatePos.length;
		for (int i = 0; i < len; i++) {
			template[templatePos[i]] = values[valuePos[i]];
		}
		return template;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "ScopeFormat '" + format + '\'';
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Formats the given string to resemble a scope variable.
	 *
	 * @param scope The string to format
	 * @return The formatted string
	 */
	public static String asVariable(String scope) {
		return SCOPE_VARIABLE_PREFIX + scope + SCOPE_VARIABLE_SUFFIX;
	}

	public static String concat(String... components) {
		return concat(defaultFilter, '.', components);
	}

	public static String concat(CharacterFilter filter, String... components) {
		return concat(filter, '.', components);
	}

	public static String concat(Character delimiter, String... components) {
		return concat(defaultFilter, delimiter, components);
	}

	/**
	 * Concatenates the given component names separated by the delimiter character. Additionally
	 * the character filter is applied to all component names.
	 *
	 * @param filter Character filter to be applied to the component names
	 * @param delimiter Delimiter to separate component names
	 * @param components Array of component names
	 * @return The concatenated component name
	 */
	public static String concat(CharacterFilter filter, Character delimiter, String... components) {
		StringBuilder sb = new StringBuilder();
		sb.append(filter.filterCharacters(components[0]));
		for (int x = 1; x < components.length; x++) {
			sb.append(delimiter);
			sb.append(filter.filterCharacters(components[x]));
		}
		return sb.toString();
	}

	protected static String valueOrNull(Object value) {
		return (value == null || (value instanceof String && ((String) value).isEmpty())) ?
				"null" : value.toString();
	}

	protected static HashMap<String, Integer> arrayToMap(String[] array) {
		HashMap<String, Integer> map = new HashMap<>(array.length);
		for (int i = 0; i < array.length; i++) {
			map.put(array[i], i);
		}
		return map;
	}

	private static int[] integerListToArray(List<Integer> list) {
		int[] array = new int[list.size()];
		int pos = 0;
		for (Integer i : list) {
			array[pos++] = i;
		}
		return array;
	}
}
