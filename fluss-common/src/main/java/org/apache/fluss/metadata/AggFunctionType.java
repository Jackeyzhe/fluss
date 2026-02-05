/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeFamily;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Aggregation function type for aggregate merge engine.
 *
 * <p>This enum represents all supported aggregation function types that can be applied to
 * non-primary key columns in aggregation merge engine tables.
 */
@PublicEvolving
public enum AggFunctionType {
    // Numeric aggregation
    SUM(Collections.emptySet(), new DataTypeFamily[] {DataTypeFamily.NUMERIC}),
    PRODUCT(Collections.emptySet(), new DataTypeFamily[] {DataTypeFamily.NUMERIC}),
    MAX(
            Collections.emptySet(),
            new DataTypeFamily[] {
                DataTypeFamily.CHARACTER_STRING, DataTypeFamily.NUMERIC, DataTypeFamily.DATETIME
            }),
    MIN(
            Collections.emptySet(),
            new DataTypeFamily[] {
                DataTypeFamily.CHARACTER_STRING, DataTypeFamily.NUMERIC, DataTypeFamily.DATETIME
            }),

    // Value selection
    LAST_VALUE(Collections.emptySet(), DataTypeFamily.values()),
    LAST_VALUE_IGNORE_NULLS(Collections.emptySet(), DataTypeFamily.values()),
    FIRST_VALUE(Collections.emptySet(), DataTypeFamily.values()),
    FIRST_VALUE_IGNORE_NULLS(Collections.emptySet(), DataTypeFamily.values()),

    // String aggregation
    LISTAGG(
            Collections.singleton(AggFunctions.PARAM_DELIMITER),
            new DataTypeFamily[] {DataTypeFamily.CHARACTER_STRING}),
    // Alias for LISTAGG - maps to same factory
    STRING_AGG(
            Collections.singleton(AggFunctions.PARAM_DELIMITER),
            new DataTypeFamily[] {DataTypeFamily.CHARACTER_STRING}),

    // Boolean aggregation
    BOOL_AND(Collections.emptySet(), new DataTypeFamily[0]),
    BOOL_OR(Collections.emptySet(), new DataTypeFamily[0]);

    private final Set<String> supportedParameters;

    private final DataTypeFamily[] supportedDataTypeFamilies;

    AggFunctionType(Set<String> supportedParameters, DataTypeFamily[] supportedDataTypeFamilies) {
        this.supportedParameters = supportedParameters;
        this.supportedDataTypeFamilies = supportedDataTypeFamilies;
    }

    /**
     * Validates a parameter value for this aggregation function.
     *
     * @param parameterName the parameter name
     * @param parameterValue the parameter value
     * @throws IllegalArgumentException if the parameter value is invalid
     */
    public void validateParameter(String parameterName, String parameterValue) {
        // Check if parameter is supported
        if (!this.supportedParameters.contains(parameterName)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Parameter '%s' is not supported for aggregation function '%s'. "
                                    + "Supported parameters: %s",
                            parameterName,
                            this,
                            this.supportedParameters.isEmpty()
                                    ? "none"
                                    : this.supportedParameters));
        }

        // Validate parameter value based on function type and parameter name
        switch (this) {
            case LISTAGG:
            case STRING_AGG:
                if (AggFunctions.PARAM_DELIMITER.equals(parameterName)) {
                    if (parameterValue == null || parameterValue.isEmpty()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Parameter '%s' for aggregation function '%s' must be a non-empty string",
                                        parameterName, this));
                    }
                }
                break;
            default:
                // No validation needed for other functions (they don't have parameters)
                break;
        }
    }

    /**
     * Validates a data type for this aggregation function.
     *
     * @param fieldType the field data type
     * @throws IllegalArgumentException if the data type is invalid
     */
    public void validateDataType(DataType fieldType) {
        switch (this) {
                // The bool_and and bool_or don't have specific DataFamily, validate them by
                // dataType directly.
            case BOOL_AND:
            case BOOL_OR:
                checkArgument(
                        fieldType instanceof BooleanType,
                        "Data type for %s column must be 'BooleanType' but was '%s'.",
                        toString(),
                        fieldType);
                break;
            default:
                checkArgument(
                        fieldType.isAnyOf(this.supportedDataTypeFamilies),
                        "Data type for %s column must be part of %s but was '%s'.",
                        toString(),
                        Arrays.deepToString(this.supportedDataTypeFamilies),
                        fieldType);
                break;
        }
    }

    /**
     * Converts a string to an AggFunctionType enum value.
     *
     * <p>This method supports multiple naming formats:
     *
     * <ul>
     *   <li>Underscore format: "last_value_ignore_nulls"
     *   <li>Hyphen format: "last-value-ignore-nulls"
     *   <li>Case insensitive matching
     * </ul>
     *
     * <p>Note: For string_agg, this will return STRING_AGG enum, but the server-side factory will
     * map it to the same implementation as listagg.
     *
     * @param name the aggregation function type name
     * @return the AggFunctionType enum value, or null if not found
     */
    public static AggFunctionType fromString(String name) {
        if (name == null || name.trim().isEmpty()) {
            return null;
        }

        // Normalize the input: convert hyphens to underscores and uppercase
        String normalized = name.replace('-', '_').toUpperCase(Locale.ROOT).trim();

        try {
            return AggFunctionType.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Converts this AggFunctionType to its string identifier.
     *
     * <p>The identifier is the lowercase name with underscores, e.g., "sum", "last_value".
     *
     * @return the identifier string
     */
    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
