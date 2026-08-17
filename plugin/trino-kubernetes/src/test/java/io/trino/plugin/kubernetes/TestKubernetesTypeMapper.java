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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.plugin.kubernetes.schema.KubernetesTypeMapper;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

final class TestKubernetesTypeMapper
{
    private static final String OPENAPI_DOCUMENT =
            """
            {
              "openapi": "3.0.0",
              "paths": {
                "/apis/example.io/v1/namespaces/{namespace}/things": {
                  "get": {
                    "responses": {
                      "200": {
                        "content": {
                          "application/json": {
                            "schema": {"$ref": "#/components/schemas/io.example.v1.ThingList"}
                          }
                        }
                      }
                    }
                  }
                }
              },
              "components": {
                "schemas": {
                  "io.example.v1.ThingList": {
                    "type": "object",
                    "properties": {
                      "items": {
                        "type": "array",
                        "items": {"allOf": [{"$ref": "#/components/schemas/io.example.v1.Thing"}], "default": {}}
                      }
                    }
                  },
                  "io.example.v1.Thing": {
                    "type": "object",
                    "properties": {
                      "apiVersion": {"type": "string"},
                      "kind": {"type": "string"},
                      "metadata": {"allOf": [{"$ref": "#/components/schemas/io.example.v1.Meta"}]},
                      "spec": {"$ref": "#/components/schemas/io.example.v1.Spec"}
                    }
                  },
                  "io.example.v1.Meta": {
                    "type": "object",
                    "properties": {
                      "name": {"type": "string"},
                      "creationTimestamp": {"allOf": [{"$ref": "#/components/schemas/io.example.v1.Time"}]},
                      "labels": {"type": "object", "additionalProperties": {"type": "string"}}
                    }
                  },
                  "io.example.v1.Time": {"type": "string", "format": "date-time"},
                  "io.example.v1.Quantity": {"oneOf": [{"type": "string"}, {"type": "number"}]},
                  "io.example.v1.Spec": {
                    "type": "object",
                    "properties": {
                      "replicas32": {"type": "integer", "format": "int32"},
                      "replicas": {"type": "integer", "format": "int64"},
                      "image": {"type": "string"},
                      "blob": {"type": "string", "format": "byte"},
                      "size": {"x-kubernetes-int-or-string": true},
                      "quantity": {"$ref": "#/components/schemas/io.example.v1.Quantity"},
                      "tags": {"type": "array", "items": {"type": "string"}},
                      "recursive": {"$ref": "#/components/schemas/io.example.v1.Spec"},
                      "unknown": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}
                    }
                  }
                }
              }
            }
            """;

    private static final ResourceDescriptor THINGS = new ResourceDescriptor("example.io", "things", "example.io", "v1", "Thing", true);

    @Test
    void testTypeMapping()
            throws Exception
    {
        JsonNode document = new ObjectMapper().readTree(OPENAPI_DOCUMENT);
        KubernetesTypeMapper mapper = new KubernetesTypeMapper(TESTING_TYPE_MANAGER);
        List<KubernetesColumnHandle> columns = mapper.columns(document, THINGS);

        Map<String, Type> types = columns.stream()
                .collect(Collectors.toMap(KubernetesColumnHandle::name, KubernetesColumnHandle::type));
        assertThat(types.keySet()).containsExactlyInAnyOrder("apiversion", "kind", "metadata", "spec");
        assertThat(types.get("apiversion")).isEqualTo(VARCHAR);

        RowType metadata = (RowType) types.get("metadata");
        assertThat(fieldType(metadata, "name")).isEqualTo(VARCHAR);
        assertThat(fieldType(metadata, "creationTimestamp")).isEqualTo(TIMESTAMP_TZ_MILLIS);
        MapType labels = (MapType) fieldType(metadata, "labels");
        assertThat(labels.getKeyType()).isEqualTo(VARCHAR);
        assertThat(labels.getValueType()).isEqualTo(VARCHAR);

        RowType spec = (RowType) types.get("spec");
        assertThat(fieldType(spec, "replicas32")).isEqualTo(INTEGER);
        assertThat(fieldType(spec, "replicas")).isEqualTo(BIGINT);
        assertThat(fieldType(spec, "image")).isEqualTo(VARCHAR);
        assertThat(fieldType(spec, "blob")).isEqualTo(VARBINARY);
        assertThat(fieldType(spec, "size")).isEqualTo(VARCHAR);
        assertThat(fieldType(spec, "quantity")).isEqualTo(VARCHAR);
        assertThat(fieldType(spec, "tags")).isEqualTo(new ArrayType(VARCHAR));
        // self-referential schema falls back to json
        assertThat(fieldType(spec, "recursive").getBaseName()).isEqualTo("json");
        assertThat(fieldType(spec, "unknown").getBaseName()).isEqualTo("json");
    }

    @Test
    void testUnknownResourceFallsBack()
            throws Exception
    {
        JsonNode document = new ObjectMapper().readTree(OPENAPI_DOCUMENT);
        KubernetesTypeMapper mapper = new KubernetesTypeMapper(TESTING_TYPE_MANAGER);
        ResourceDescriptor unknown = new ResourceDescriptor("example.io", "missing", "example.io", "v1", "Missing", true);
        List<KubernetesColumnHandle> columns = mapper.columns(document, unknown);
        assertThat(columns).extracting(KubernetesColumnHandle::name)
                .containsExactly("apiversion", "kind", "metadata");
    }

    private static Type fieldType(RowType type, String fieldName)
    {
        Optional<RowType.Field> field = type.getFields().stream()
                .filter(candidate -> candidate.getName().orElseThrow().equals(fieldName))
                .findFirst();
        assertThat(field).as("field %s in %s", fieldName, type).isPresent();
        return field.get().getType();
    }
}
