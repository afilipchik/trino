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
package io.trino.plugin.kubernetes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_SCHEMA_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;

/**
 * Converts Trino values back to Kubernetes API JSON, the inverse of {@link JsonValueDecoder}.
 * Null row fields are omitted from the produced objects so that writes stay minimal.
 */
public final class JsonValueEncoder
{
    private static final ObjectMapper MAPPER = new JsonMapperProvider().get();
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private JsonValueEncoder() {}

    /**
     * @return the JSON value, or {@code null} when the position is SQL NULL
     */
    public static JsonNode toJson(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (type instanceof RowType rowType) {
            SqlRow row = rowType.getObject(block, position);
            ObjectNode object = NODES.objectNode();
            List<RowType.Field> fields = rowType.getFields();
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                JsonNode fieldValue = toJson(field.getType(), row.getRawFieldBlock(i), row.getRawIndex());
                if (fieldValue != null) {
                    object.set(field.getName().orElseThrow(), fieldValue);
                }
            }
            return object;
        }
        if (type instanceof MapType mapType) {
            SqlMap map = mapType.getObject(block, position);
            ObjectNode object = NODES.objectNode();
            int offset = map.getRawOffset();
            Block keyBlock = map.getRawKeyBlock();
            Block valueBlock = map.getRawValueBlock();
            for (int i = 0; i < map.getSize(); i++) {
                JsonNode key = toJson(mapType.getKeyType(), keyBlock, offset + i);
                JsonNode mapValue = toJson(mapType.getValueType(), valueBlock, offset + i);
                if (key != null) {
                    object.set(key.isTextual() ? key.asText() : key.toString(), mapValue == null ? NODES.nullNode() : mapValue);
                }
            }
            return object;
        }
        if (type instanceof ArrayType arrayType) {
            Block array = arrayType.getObject(block, position);
            ArrayNode result = NODES.arrayNode();
            for (int i = 0; i < array.getPositionCount(); i++) {
                JsonNode element = toJson(arrayType.getElementType(), array, i);
                result.add(element == null ? NODES.nullNode() : element);
            }
            return result;
        }
        if (type instanceof VarcharType varcharType) {
            return NODES.textNode(varcharType.getSlice(block, position).toStringUtf8());
        }
        if (type instanceof VarbinaryType varbinaryType) {
            return NODES.textNode(Base64.getEncoder().encodeToString(varbinaryType.getSlice(block, position).getBytes()));
        }
        if (type.equals(BOOLEAN)) {
            return NODES.booleanNode(BOOLEAN.getBoolean(block, position));
        }
        if (type.equals(INTEGER)) {
            return NODES.numberNode(INTEGER.getInt(block, position));
        }
        if (type.equals(BIGINT)) {
            return NODES.numberNode(BIGINT.getLong(block, position));
        }
        if (type.equals(DOUBLE)) {
            return NODES.numberNode(DOUBLE.getDouble(block, position));
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            long epochMillis = unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position));
            return NODES.textNode(Instant.ofEpochMilli(epochMillis).toString());
        }
        if (type.getBaseName().equals(JSON)) {
            try {
                return MAPPER.readTree(type.getSlice(block, position).toStringUtf8());
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(KUBERNETES_SCHEMA_ERROR, "Invalid JSON value", e);
            }
        }
        throw new TrinoException(KUBERNETES_SCHEMA_ERROR, "Unsupported type for Kubernetes value: " + type);
    }
}
