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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.CalciteParser;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of {@link Parser} that uses Calcite.
 */
public class ParserImpl implements Parser {

	private final CatalogManager catalogManager;

	// we use supplier pattern here in order to use the most up to
	// date configuration. Users might change the parser configuration in a TableConfig in between
	// multiple statements parsing
	private final Supplier<FlinkPlannerImpl> validatorSupplier;
	private final Supplier<CalciteParser> calciteParserSupplier;
	private final Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator;

	public ParserImpl(
			CatalogManager catalogManager,
			Supplier<FlinkPlannerImpl> validatorSupplier,
			Supplier<CalciteParser> calciteParserSupplier,
			Function<TableSchema, SqlExprToRexConverter> sqlExprToRexConverterCreator) {
		this.catalogManager = catalogManager;
		this.validatorSupplier = validatorSupplier;
		this.calciteParserSupplier = calciteParserSupplier;
		this.sqlExprToRexConverterCreator = sqlExprToRexConverterCreator;
	}
	// HeryCode:解析 sql 语句 返回操作列表
	@Override
	public List<Operation> parse(String statement) {
		// HeryCode:获取解析器
		CalciteParser parser = calciteParserSupplier.get();
		// HeryCode:获取flink sql 的planner
		FlinkPlannerImpl planner = validatorSupplier.get();
		// parse the sql query
		// HeryCode:解析sql 语句生成SqlNode，也就是sql 解析树
		SqlNode parsed = parser.parse(statement);
		// HeryCode:通过执行计划，catalog、sql树 生成Operation，Operation是一个接口，这里指的是查询类操作QueryOperation 的实现
		Operation operation = SqlToOperationConverter.convert(planner, catalogManager, parsed)
			.orElseThrow(() -> new TableException("Unsupported query: " + statement));
		return Collections.singletonList(operation);
	}
	// HeryCode: 解析标识符
	@Override
	public UnresolvedIdentifier parseIdentifier(String identifier) {
		CalciteParser parser = calciteParserSupplier.get();
		SqlIdentifier sqlIdentifier = parser.parseIdentifier(identifier);
		return UnresolvedIdentifier.of(sqlIdentifier.names);
	}
	// HeryCode:解析sql 异常
	@Override
	public ResolvedExpression parseSqlExpression(String sqlExpression, TableSchema inputSchema) {
		SqlExprToRexConverter sqlExprToRexConverter = sqlExprToRexConverterCreator.apply(inputSchema);
		RexNode rexNode = sqlExprToRexConverter.convertToRexNode(sqlExpression);
		LogicalType logicalType = FlinkTypeFactory.toLogicalType(rexNode.getType());
		return new RexNodeExpression(rexNode, TypeConversions.fromLogicalToDataType(logicalType));
	}
}
