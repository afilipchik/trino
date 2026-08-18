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
package io.trino.plugin.kubernetes.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.kubernetes.KubernetesColumnHandle;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Maps the OpenAPI v3 JSON schema of a Kubernetes resource to Trino column types.
 * Objects become rows (field name case is preserved), {@code additionalProperties}
 * become maps, unresolvable or recursive schemas fall back to the {@code json} type.
 */
public class KubernetesTypeMapper
{
    private static final String COMPONENTS_PREFIX = "#/components/schemas/";
    private static final int MAX_DEPTH = 32;

    private final TypeManager typeManager;
    private final Type jsonType;

    @Inject
    public KubernetesTypeMapper(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.jsonType = typeManager.getType(new TypeDescriptor(JSON));
    }

    public Type jsonType()
    {
        return jsonType;
    }

    /**
     * Columns for the resource, in the order the OpenAPI object schema declares its
     * top-level properties. Returns fallback generic columns if the schema cannot be found.
     */
    public List<KubernetesColumnHandle> columns(JsonNode openApiDocument, ResourceDescriptor resource)
    {
        JsonNode itemSchema = resolveItemSchema(openApiDocument, resource);
        if (itemSchema == null) {
            return ImmutableList.of(
                    new KubernetesColumnHandle("apiversion", "apiVersion", VARCHAR, false),
                    new KubernetesColumnHandle("kind", "kind", VARCHAR, false),
                    new KubernetesColumnHandle("metadata", "metadata", jsonType, false));
        }

        JsonNode components = openApiDocument.path("components").path("schemas");
        ImmutableList.Builder<KubernetesColumnHandle> columns = ImmutableList.builder();
        Set<String> seen = new HashSet<>();
        itemSchema.path("properties").properties().forEach(property -> {
            String columnName = property.getKey().toLowerCase(Locale.ENGLISH);
            if (!seen.add(columnName)) {
                return;
            }
            Type type = mapType(property.getValue(), components, new ArrayDeque<>(), 0);
            columns.add(new KubernetesColumnHandle(columnName, property.getKey(), type, false));
        });
        return columns.build();
    }

    private JsonNode resolveItemSchema(JsonNode document, ResourceDescriptor resource)
    {
        String prefix = "/" + resource.groupVersionPath();
        String listPath = resource.namespaced()
                ? prefix + "/namespaces/{namespace}/" + resource.tableName()
                : prefix + "/" + resource.tableName();
        JsonNode responseSchema = document.path("paths").path(listPath)
                .path("get").path("responses").path("200")
                .path("content").path("application/json").path("schema");
        if (responseSchema.isMissingNode()) {
            return null;
        }

        JsonNode components = document.path("components").path("schemas");
        JsonNode listSchema = dereference(responseSchema, components);
        if (listSchema == null) {
            return null;
        }
        JsonNode itemsSchema = dereference(listSchema.path("properties").path("items").path("items"), components);
        if (itemsSchema == null || !itemsSchema.path("properties").isObject()) {
            return null;
        }
        return itemsSchema;
    }

    private static JsonNode dereference(JsonNode schema, JsonNode components)
    {
        JsonNode current = unwrapAllOf(schema);
        for (int i = 0; i < MAX_DEPTH; i++) {
            String ref = current.path("$ref").asText("");
            if (ref.isEmpty()) {
                return current.isObject() ? current : null;
            }
            if (!ref.startsWith(COMPONENTS_PREFIX)) {
                return null;
            }
            current = unwrapAllOf(components.path(ref.substring(COMPONENTS_PREFIX.length())));
        }
        return null;
    }

    private static JsonNode unwrapAllOf(JsonNode schema)
    {
        JsonNode allOf = schema.path("allOf");
        if (allOf.isArray() && allOf.size() == 1) {
            return allOf.get(0);
        }
        return schema;
    }

    private Type mapType(JsonNode schema, JsonNode components, Deque<String> referenceStack, int depth)
    {
        if (depth > MAX_DEPTH) {
            return jsonType;
        }
        JsonNode current = unwrapAllOf(schema);

        String ref = current.path("$ref").asText("");
        if (!ref.isEmpty()) {
            if (!ref.startsWith(COMPONENTS_PREFIX)) {
                return jsonType;
            }
            String name = ref.substring(COMPONENTS_PREFIX.length());
            if (referenceStack.contains(name)) {
                return jsonType;
            }
            JsonNode resolved = components.path(name);
            if (!resolved.isObject()) {
                return jsonType;
            }
            referenceStack.push(name);
            Type type = mapType(resolved, components, referenceStack, depth + 1);
            referenceStack.pop();
            return type;
        }

        if (current.path("x-kubernetes-int-or-string").asBoolean(false)) {
            return VARCHAR;
        }

        String type = current.path("type").asText("");
        return switch (type) {
            case "object" -> mapObject(current, components, referenceStack, depth);
            case "array" -> {
                JsonNode items = current.path("items");
                if (items.isMissingNode()) {
                    yield jsonType;
                }
                yield new ArrayType(mapType(items, components, referenceStack, depth + 1));
            }
            case "string" -> switch (current.path("format").asText("")) {
                case "date-time" -> TIMESTAMP_MILLIS;
                case "byte" -> VARBINARY;
                default -> VARCHAR;
            };
            case "integer" -> "int32".equals(current.path("format").asText("")) ? INTEGER : BIGINT;
            case "number" -> DOUBLE;
            case "boolean" -> BOOLEAN;
            default -> {
                // untyped: int-or-string unions and x-kubernetes-preserve-unknown-fields end up here
                if (current.path("oneOf").isArray() || current.path("anyOf").isArray()) {
                    yield VARCHAR;
                }
                yield jsonType;
            }
        };
    }

    private Type mapObject(JsonNode schema, JsonNode components, Deque<String> referenceStack, int depth)
    {
        JsonNode properties = schema.path("properties");
        if (properties.isObject() && !properties.isEmpty()) {
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            properties.properties().forEach(property ->
                    fields.add(RowType.field(property.getKey(), mapType(property.getValue(), components, referenceStack, depth + 1))));
            return RowType.from(fields.build());
        }
        JsonNode additionalProperties = schema.path("additionalProperties");
        if (additionalProperties.isObject()) {
            Type valueType = mapType(additionalProperties, components, referenceStack, depth + 1);
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                    TypeParameter.typeParameter(VARCHAR.getTypeDescriptor()),
                    TypeParameter.typeParameter(valueType.getTypeDescriptor())));
        }
        return jsonType;
    }
}
