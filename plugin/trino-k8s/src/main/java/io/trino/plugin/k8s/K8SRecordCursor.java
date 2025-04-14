/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.lang.reflect.Field;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class K8SRecordCursor
        implements RecordCursor
{
    private final K8SClient k8sClient;
    private final K8SSplit split;
    private final List<K8SColumnHandle> columnHandles;
    private final ObjectMapper objectMapper;
    private final TypeManager typeManager;
    private List<Map<String, Object>> rows;
    private int currentPosition = -1;
    private Block[] currentBlocks;
    private final List<Object> values;
    private final List<Type> types;

    public K8SRecordCursor(K8SClient k8sClient, K8SSplit split, List<K8SColumnHandle> columnHandles, String schemaName, String tableName, ObjectMapper objectMapper, TypeManager typeManager, List<Object> values, List<Type> types)
    {
        this.k8sClient = requireNonNull(k8sClient, "k8sClient is null");
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.objectMapper = objectMapper;
        this.typeManager = typeManager;
        this.values = values;
        this.types = types;
        
        // Get the rows for this table
        this.rows = k8sClient.getResourceAsList(schemaName, tableName);
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentPosition++;
        if (currentPosition >= rows.size()) {
            return false;
        }

        Map<String, Object> row = rows.get(currentPosition);
        
        // Create a block for each column
        Block[] blocks = new Block[columnHandles.size()];
        
        for (int i = 0; i < columnHandles.size(); i++) {
            K8SColumnHandle columnHandle = columnHandles.get(i);
            Type type = columnHandle.getColumnType();
            Object value = row.get(columnHandle.getColumnName());
            
            if (value == null) {
                blocks[i] = null;
                continue;
            }
            
            if (type instanceof RowType rowType) {
                try {
                    BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);
                    ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
                        for (int j = 0; j < rowType.getFields().size(); j++) {
                            RowType.Field field = rowType.getFields().get(j);
                            String fieldName = field.getName().orElse("field" + j);
                            Object fieldValue = getFieldValue(value, fieldName);
                            writeValue(fieldValue, field.getType(), fieldBuilders.get(j));
                        }
                    });
                    blocks[i] = blockBuilder.build();
                }
                catch (Exception e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to convert object to row", e);
                }
            }
            else {
                BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
                writeValue(value, type, blockBuilder);
                blocks[i] = blockBuilder.build();
            }
        }
        
        // Store the blocks for this row
        currentBlocks = blocks;
        return true;
    }

    private Object getFieldValue(Object obj, String fieldName)
    {
        if (obj == null) {
            return null;
        }
        
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).get(fieldName);
        }
        
        try {
            // Try to get the field using reflection
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(obj);
        }
        catch (Exception e) {
            // If reflection fails, try to get the field from a map representation
            try {
                Map<String, Object> map = objectMapper.convertValue(obj, Map.class);
                return map.get(fieldName);
            }
            catch (Exception ex) {
                return null;
            }
        }
    }

    private void writeJsonValue(JsonNode node, Type type, BlockBuilder builder)
    {
        if (node == null || node.isNull()) {
            builder.appendNull();
            return;
        }

        if (type instanceof RowType rowType) {
            ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    RowType.Field field = rowType.getFields().get(i);
                    String fieldName = field.getName().orElse("field" + i);
                    JsonNode fieldNode = node.get(fieldName);
                    writeJsonValue(fieldNode, field.getType(), fieldBuilders.get(i));
                }
            });
        }
        else if (type instanceof ArrayType arrayType) {
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                for (JsonNode element : node) {
                    writeJsonValue(element, arrayType.getElementType(), elementBuilder);
                }
            });
        }
        else if (type.equals(VARCHAR)) {
            type.writeSlice(builder, Slices.utf8Slice(node.asText()));
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(builder, node.asLong());
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, node.asDouble());
        }
        else if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, node.asBoolean());
        }
        else if (type.equals(getJsonMapType())) {
            // Handle JSON type by converting to a string representation
            try {
                String jsonString = objectMapper.writeValueAsString(node);
                type.writeSlice(builder, Slices.utf8Slice(jsonString));
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to convert JSON node to string", e);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported type: " + type);
        }
    }

    private void writeValue(Object value, Type type, BlockBuilder builder)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }

        if (type instanceof RowType rowType) {
            try {
                ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> {
                    for (int i = 0; i < rowType.getFields().size(); i++) {
                        RowType.Field field = rowType.getFields().get(i);
                        String fieldName = field.getName().orElse("field" + i);
                        Object fieldValue = getFieldValue(value, fieldName);
                        writeValue(fieldValue, field.getType(), fieldBuilders.get(i));
                    }
                });
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to convert object to row", e);
            }
        }
        else if (type instanceof ArrayType arrayType) {
            List<?> list;
            if (value instanceof List) {
                list = (List<?>) value;
            }
            else {
                // If value is not a List, try to convert it to one
                try {
                    list = objectMapper.convertValue(value, List.class);
                }
                catch (Exception e) {
                    // If conversion fails, create a single-element list
                    list = List.of(value);
                }
            }
            
            final List<?> finalList = list;
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                for (Object element : finalList) {
                    writeValue(element, arrayType.getElementType(), elementBuilder);
                }
            });
        }
        else if (type.getTypeSignature().getBase().equals("timestamp")) {
            // Handle timestamp values
            if (value instanceof java.sql.Timestamp) {
                type.writeLong(builder, ((java.sql.Timestamp) value).getTime() * 1000L); // Convert to microseconds
            }
            else if (value instanceof java.time.OffsetDateTime) {
                type.writeLong(builder, ((java.time.OffsetDateTime) value).toInstant().toEpochMilli() * 1000L);
            }
            else if (value instanceof java.util.Date) {
                type.writeLong(builder, ((java.util.Date) value).getTime() * 1000L);
            }
            else if (value instanceof Number) {
                // Assume epoch seconds if it's a number
                type.writeLong(builder, ((Number) value).longValue() * 1_000_000L); // Convert seconds to microseconds
            }
            else {
                // Try to parse as string
                try {
                    java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.toString());
                    type.writeLong(builder, ts.getTime() * 1000L);
                }
                catch (IllegalArgumentException e) {
                    builder.appendNull();
                }
            }
        }
        else if (type.equals(VARCHAR)) {
            if (value instanceof io.kubernetes.client.custom.IntOrString intOrString) {
                if (intOrString.isInteger()) {
                    VARCHAR.writeSlice(builder, Slices.utf8Slice(String.valueOf(intOrString.getIntValue())));
                }
                else {
                    VARCHAR.writeSlice(builder, Slices.utf8Slice(intOrString.getStrValue()));
                }
            }
            else {
                VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
            }
        }
        else if (type.equals(INTEGER)) {
            if (value instanceof Number number) {
                INTEGER.writeLong(builder, number.longValue());
            }
            else if (value instanceof io.kubernetes.client.custom.IntOrString intOrString && intOrString.isInteger()) {
                INTEGER.writeLong(builder, intOrString.getIntValue());
            }
            else {
                try {
                    INTEGER.writeLong(builder, Long.parseLong(value.toString()));
                }
                catch (NumberFormatException e) {
                    builder.appendNull();
                }
            }
        }
        else if (type.equals(DOUBLE)) {
            if (value instanceof Number number) {
                DOUBLE.writeDouble(builder, number.doubleValue());
            }
            else {
                try {
                    DOUBLE.writeDouble(builder, Double.parseDouble(value.toString()));
                }
                catch (NumberFormatException e) {
                    builder.appendNull();
                }
            }
        }
        else if (type.equals(BOOLEAN)) {
            if (value instanceof Boolean booleanValue) {
                BOOLEAN.writeBoolean(builder, booleanValue);
            }
            else {
                BOOLEAN.writeBoolean(builder, Boolean.parseBoolean(value.toString()));
            }
        }
        else {
            // For any other type, convert to string
            VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
        }
    }
    
    private Type getJsonMapType()
    {
        return typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    public boolean isNull(int field)
    {
        return currentBlocks[field] == null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        if (currentBlocks[field] == null) {
            return false;
        }
        
        Type type = columnHandles.get(field).getColumnType();
        if (type.equals(BOOLEAN)) {
            return type.getBoolean(currentBlocks[field], 0);
        }
        
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Type " + type + " does not support getBoolean");
    }

    @Override
    public long getLong(int field)
    {
        if (currentBlocks[field] == null) {
            return 0;
        }
        
        Type type = columnHandles.get(field).getColumnType();
        if (type.equals(INTEGER)) {
            return type.getLong(currentBlocks[field], 0);
        }
        
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Type " + type + " does not support getLong");
    }

    @Override
    public double getDouble(int field)
    {
        if (currentBlocks[field] == null) {
            return 0.0;
        }
        
        Type type = columnHandles.get(field).getColumnType();
        if (type.equals(DOUBLE)) {
            return type.getDouble(currentBlocks[field], 0);
        }
        
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Type " + type + " does not support getDouble");
    }

    @Override
    public Slice getSlice(int field)
    {
        if (currentBlocks[field] == null) {
            return null;
        }
        
        Type type = columnHandles.get(field).getColumnType();
        if (type.equals(VARCHAR) || type.equals(VARBINARY)) {
            return type.getSlice(currentBlocks[field], 0);
        }
        
        // Handle JSON type by returning string value directly
        if (type.equals(getJsonMapType())) {
            try {
                String value = type.getSlice(currentBlocks[field], 0).toStringUtf8();
                return Slices.utf8Slice(value);
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get JSON string value", e);
            }
        }
        
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Type " + type + " does not support getSlice");
    }

    @Override
    public Object getObject(int field)
    {
        if (currentBlocks[field] == null) {
            return null;
        }
        
        Type type = columnHandles.get(field).getColumnType();
        if (type instanceof RowType || type instanceof ArrayType) {
            return type.getObject(currentBlocks[field], 0);
        }
        
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Type " + type + " does not support getObject");
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public void close()
    {
        // No resources to close
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }
}
