/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.hive.readers.filter;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static org.apache.drill.exec.expr.fn.impl.MappifyUtility.fieldValue;

public class HiveFilterBuilder extends AbstractExprVisitor<SearchArgument.Builder, Void,
    RuntimeException> {
  private static final Logger logger = LoggerFactory.getLogger(HiveFilterBuilder.class);

  final private HiveScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = false;

  private static Boolean nullComparatorSupported;

  private SearchArgument.Builder builder;

  private HashMap<String, SqlTypeName> dataTypeMap;

  HiveFilterBuilder(HiveScan groupScan, LogicalExpression le, HashMap<String, SqlTypeName> dataTypeMap) {
    this.groupScan = groupScan;
    this.le = le;
    this.dataTypeMap = dataTypeMap;
    this.builder = SearchArgumentFactory.newBuilder();
  }

  public HiveFilter parseTree() {
    SearchArgument.Builder accept = le.accept(this, null);
    SearchArgument searchArgument = builder.build();
    if (accept != null) {
      searchArgument = accept.build();
    }
    return new HiveFilter(searchArgument);
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public SearchArgument.Builder visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    if (e instanceof FieldReference) {
      String fieldName = ((FieldReference) e).getAsNamePart().getName();
      SqlTypeName sqlTypeName = dataTypeMap.get(fieldName);
      switch (sqlTypeName) {
      case BOOLEAN:
        PredicateLeaf.Type valueType = convertLeafType(sqlTypeName);
        builder.equals(fieldName, valueType, true).end();
        break;
      default:
        builder.literal(SearchArgument.TruthValue.YES_NO_NULL);
      }
    }
    return builder;
  }

  @Override
  public SearchArgument.Builder visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public SearchArgument.Builder visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    String functionName = call.getName();
    List<LogicalExpression> args = call.args();
    if (HiveCompareFunctionsProcessor.isCompareFunction(functionName)) {
      if (nullComparatorSupported == null) {
        //For Support Hive Different versions
        nullComparatorSupported =
            groupScan.getHiveConf().getBoolean("drill.hive.supports.null" + ".comparator", true);
      }
      HiveCompareFunctionsProcessor processor =
          HiveCompareFunctionsProcessor.createFunctionsProcessorInstance(call,
              nullComparatorSupported);
      if (processor.isSuccess()) {
        return buildSearchArgument(call, processor);
      }
    } else {
      switch (functionName) {
      case FunctionNames.AND:
        builder.startAnd();
        break;
      case FunctionNames.OR:
        builder.startOr();
        break;
      default:
        logger.warn("Unsupported logical operator:{} for push down", functionName);
        return builder;
      }
      for (int i = 0; i < args.size(); ++i) {
        args.get(i).accept(this, null);
      }
      builder.end();
    }
    return builder;
  }

  private SearchArgument.Builder buildSearchArgument(FunctionCall call,
      HiveCompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    Object fieldValue = processor.getValue();
    PredicateLeaf.Type valueType = processor.getValueType();
    SqlTypeName sqlTypeName = dataTypeMap.get(field.getAsNamePart().getName());
    if (fieldValue == null) {
      valueType = convertLeafType(sqlTypeName);
    }
    switch (functionName) {
    case FunctionNames.EQ:
      builder.startAnd().equals(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.NE:
      builder.startNot().equals(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.GE:
      builder.startNot().lessThan(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.GT:
      builder.startNot().lessThanEquals(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.LE:
      builder.startAnd().lessThanEquals(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.LT:
      builder.startAnd().lessThan(field.getAsNamePart().getName(), valueType, fieldValue).end();
      break;
    case FunctionNames.IS_NULL:
    case "isNull":
    case "is null":
      builder.startAnd().isNull(field.getAsNamePart().getName(), valueType).end();
      break;
    case FunctionNames.IS_NOT_NULL:
    case "isNotNull":
    case "is not null":
      builder.startNot().isNull(field.getAsNamePart().getName(), valueType).end();
      break;
    case FunctionNames.NOT:
      builder.startNot().equals(field.getAsNamePart().getName(), valueType, true).end();
      break;
    }
    return builder;
  }

  private  PredicateLeaf.Type convertLeafType(SqlTypeName sqlTypeName) {
    PredicateLeaf.Type type = null;
    switch (sqlTypeName) {
    case BOOLEAN:
      type = PredicateLeaf.Type.BOOLEAN;
      break;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      type = PredicateLeaf.Type.LONG;
      break;
    case DOUBLE:
    case FLOAT:
      type = PredicateLeaf.Type.FLOAT;
      break;
    case DECIMAL:
      type = PredicateLeaf.Type.DECIMAL;
      break;
    case DATE:
      type = PredicateLeaf.Type.DATE;
      break;
    case TIMESTAMP:
      type = PredicateLeaf.Type.TIMESTAMP;
      break;
    case CHAR:
    case VARCHAR:
      type = PredicateLeaf.Type.STRING;
      break;
    default:
      throw new RuntimeException("Not support push down type:" + sqlTypeName.getName());
    }
    return type;
  }
}
