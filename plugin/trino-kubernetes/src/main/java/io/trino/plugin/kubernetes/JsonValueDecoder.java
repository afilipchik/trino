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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_SCHEMA_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;

/**
 * Writes Kubernetes API JSON values into Trino blocks according to the mapped types.
 */
public final class JsonValueDecoder
{
    private JsonValueDecoder() {}

    public static void appendTo(Type type, JsonNode value, BlockBuilder output)
    {
        if (value == null || value.isNull() || value.isMissingNode()) {
            output.appendNull();
            return;
        }

        if (type instanceof RowType rowType) {
            if (!value.isObject()) {
                output.appendNull();
                return;
            }
            List<RowType.Field> fields = rowType.getFields();
            ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    appendTo(field.getType(), value.get(field.getName().orElseThrow()), fieldBuilders.get(i));
                }
            });
            return;
        }
        if (type instanceof MapType mapType) {
            if (!value.isObject()) {
                output.appendNull();
                return;
            }
            ((MapBlockBuilder) output).buildEntry((keyBuilder, valueBuilder) -> {
                for (Map.Entry<String, JsonNode> entry : value.properties()) {
                    appendTo(mapType.getKeyType(), TextNode.valueOf(entry.getKey()), keyBuilder);
                    appendTo(mapType.getValueType(), entry.getValue(), valueBuilder);
                }
            });
            return;
        }
        if (type instanceof ArrayType arrayType) {
            if (!value.isArray()) {
                output.appendNull();
                return;
            }
            ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
                for (JsonNode element : value) {
                    appendTo(arrayType.getElementType(), element, elementBuilder);
                }
            });
            return;
        }
        if (type instanceof VarcharType varcharType) {
            String text = value.isTextual() ? value.asText() : value.toString();
            varcharType.writeSlice(output, utf8Slice(text));
            return;
        }
        if (type instanceof VarbinaryType varbinaryType) {
            try {
                varbinaryType.writeSlice(output, wrappedBuffer(Base64.getDecoder().decode(value.asText())));
            }
            catch (IllegalArgumentException _) {
                output.appendNull();
            }
            return;
        }
        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(output, value.asBoolean());
            return;
        }
        if (type.equals(INTEGER)) {
            INTEGER.writeLong(output, value.asInt());
            return;
        }
        if (type.equals(BIGINT)) {
            BIGINT.writeLong(output, value.asLong());
            return;
        }
        if (type.equals(DOUBLE)) {
            DOUBLE.writeDouble(output, value.asDouble());
            return;
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            try {
                long epochMillis = OffsetDateTime.parse(value.asText()).toInstant().toEpochMilli();
                TIMESTAMP_MILLIS.writeLong(output, epochMillis * MICROSECONDS_PER_MILLISECOND);
            }
            catch (DateTimeParseException _) {
                output.appendNull();
            }
            return;
        }
        if (type.getBaseName().equals(JSON)) {
            type.writeSlice(output, jsonParse(utf8Slice(value.toString())));
            return;
        }
        throw new TrinoException(KUBERNETES_SCHEMA_ERROR, "Unsupported type for Kubernetes value: " + type);
    }
}
