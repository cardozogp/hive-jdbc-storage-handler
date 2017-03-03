/*
 * Copyright 2012-2014 Qubit Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubitproducts.hive.storage.jdbc;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.serializer.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qubitproducts.hive.storage.jdbc.conf.JdbcStorageConfig;

import java.beans.XMLDecoder;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Translates the hive query condition into a condition that can be run on the underlying database
 */
public class QueryConditionBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryConditionBuilder.class);
    private static final String EMPTY_STRING = "";
    private static QueryConditionBuilder instance = null;


    public static QueryConditionBuilder getInstance() {
        if (instance == null) {
            instance = new QueryConditionBuilder();
        }

        return instance;
    }


    private QueryConditionBuilder() {

    }


    public String buildCondition(Configuration conf) {
        if (conf == null) {
            return EMPTY_STRING;
        }

        String filterXml = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
        String hiveColumns = conf.get(serdeConstants.LIST_COLUMNS);
        String columnMapping = conf.get(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName());

        if ((filterXml == null) || ((columnMapping == null) && (hiveColumns == null))) {
            return EMPTY_STRING;
        }

        if (hiveColumns == null) {
            hiveColumns = "";
        }

        Map<String, String> columnMap = buildColumnMapping(columnMapping, hiveColumns);
        String condition = createConditionString(filterXml, columnMap);
        return condition;
    }


    /*
     * Build a Hive-to-X column mapping,
     *
     */
    private Map<String, String> buildColumnMapping(String columnMapping, String hiveColumns) {
        if ((columnMapping == null) || (columnMapping.trim().isEmpty())) {
            return createIdentityMap(hiveColumns);
        }

        Map<String, String> columnMap = new HashMap<String, String>();
        String[] mappingPairs = columnMapping.toLowerCase().split(",");
        for (String mapPair : mappingPairs) {
            String[] columns = mapPair.split("=");
            columnMap.put(columns[0].trim(), columns[1].trim());
        }

        return columnMap;
    }


    /*
     * When no mapping is defined, it is assumed that the hive column names are equivalent to the column names in the
     * underlying table
     */
    private Map<String, String> createIdentityMap(String hiveColumns) {
        Map<String, String> columnMap = new HashMap<String, String>();
        String[] columns = hiveColumns.toLowerCase().split(",");

        for (String col : columns) {
            columnMap.put(col.trim(), col.trim());
        }

        return columnMap;
    }


    /*
     * Walk to Hive AST and translate the hive column names to their equivalent mappings. This is basically a cheat.
     *
     */
    private String createConditionString(String filterXml, Map<String, String> columnMap) {
        if ((filterXml == null) || (filterXml.trim().isEmpty())) {
            return EMPTY_STRING;
        }

        // [aago] filterXml is not longer in XML but a serialized format
        ExprNodeDesc conditionNode = Utilities.deserializeExpression(filterXml);
        walkTreeAndTranslateColumnNames(conditionNode, columnMap);
        return conditionNode.getExprString();
    }


    /*
     * Translate column names by walking the AST
     */
    private void walkTreeAndTranslateColumnNames(ExprNodeDesc node, Map<String, String> columnMap) {
        if (node == null) {
            return;
        }

        if (node instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc column = (ExprNodeColumnDesc) node;
            String hiveColumnName = column.getColumn().toLowerCase();
            if (columnMap.containsKey(hiveColumnName)) {
                String dbColumnName = columnMap.get(hiveColumnName);
                String finalName = formatColumnName(dbColumnName);
                column.setColumn(finalName);
            }
        }
        else {
            if (node.getChildren() != null) {
                for (ExprNodeDesc childNode : node.getChildren()) {
                    walkTreeAndTranslateColumnNames(childNode, columnMap);
                }
            }
        }
    }


    /**
     * This is an ugly hack for handling date column types because Hive doesn't have a built-in type for dates
     */
    private String formatColumnName(String dbColumnName) {
        if (dbColumnName.contains(":")) {
            String[] typeSplit = dbColumnName.split(":");

            /* [aago] This not still true, right?
            if (typeSplit[1].equalsIgnoreCase("date")) {
                return "{d " + typeSplit[0] + "}";
            }
            */

            return typeSplit[0];
        }
        else {
            return dbColumnName;
        }
    }

    public static void main(String[] args) {
        String filterXml = "AQEAamF2YS51dGlsLkFycmF5TGlz9AECAQFvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnBsYW4uRXhwck5vZGVDb2x1bW5EZXPjAQFjb21wYW55X2nkAAABZGltX2NvbXBhbnmyAQJvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnNlcmRlMi50eXBlaW5mby5QcmltaXRpdmVUeXBlSW5m7wEBc3RyaW7nAQNvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnBsYW4uRXhwck5vZGVDb25zdGFudERlc+MBAQIBAWlu9AJkAQRvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLnVkZi5nZW5lcmljLkdlbmVyaWNVREZPUExlc3NUaGHuAQAAAYI8AUxFU1MgVEhBzgEFb3JnLmFwYWNoZS5oYWRvb3AuaW8uQm9vbGVhbldyaXRhYmzlAQAAAQIBAWJvb2xlYe4=";
        String decoded = new String(Base64.decodeBase64(filterXml));
        System.out.println("BASE64 DECODED: " + decoded);
    }
}
