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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_INVALID_WRITE;

/**
 * Builds Kubernetes object JSON from a page position using the table's data columns.
 * A non-null {@code manifest} column becomes the base object and the other columns
 * overlay it, so partial manifests and typed columns compose.
 */
public final class KubernetesObjectBuilder
{
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    private static final ObjectMapper MAPPER = new JsonMapperProvider().get();
    private static final List<String> SERVER_POPULATED_METADATA_FIELDS =
            List.of("resourceVersion", "uid", "creationTimestamp", "managedFields", "generation", "selfLink", "deletionTimestamp", "deletionGracePeriodSeconds");

    private KubernetesObjectBuilder() {}

    public static ObjectNode buildObject(ResourceDescriptor resource, List<KubernetesColumnHandle> columns, Page page, int position)
    {
        return buildObject(resource, columns, page, position, columns.stream()
                .map(KubernetesColumnHandle::name)
                .collect(toImmutableSet()));
    }

    /**
     * @param includedColumns names of columns to apply; other columns are ignored even
     *         if the page carries values for them
     */
    public static ObjectNode buildObject(ResourceDescriptor resource, List<KubernetesColumnHandle> columns, Page page, int position, Set<String> includedColumns)
    {
        ObjectNode object = manifestBase(columns, page, position, includedColumns);
        String syntheticName = null;
        String syntheticNamespace = null;
        for (int channel = 0; channel < columns.size(); channel++) {
            KubernetesColumnHandle column = columns.get(channel);
            if (!includedColumns.contains(column.name()) || column.name().equals(KubernetesColumns.MANIFEST_COLUMN)) {
                continue;
            }
            if (KubernetesColumns.isSynthetic(column)) {
                JsonNode value = JsonValueEncoder.toJson(column.type(), page.getBlock(channel), position);
                if (value != null && value.isTextual()) {
                    if (column.name().equals(KubernetesColumns.NAME_COLUMN)) {
                        syntheticName = value.asText();
                    }
                    else if (column.name().equals(KubernetesColumns.NAMESPACE_COLUMN)) {
                        syntheticNamespace = value.asText();
                    }
                }
                continue;
            }
            JsonNode value = JsonValueEncoder.toJson(column.type(), page.getBlock(channel), position);
            if (value != null && !(value.isObject() && value.isEmpty())) {
                object.set(column.jsonName(), value);
            }
        }
        if (syntheticName != null || syntheticNamespace != null) {
            // the synthetic name/namespace columns take precedence over the metadata row,
            // so SET name = ... is never silently ignored
            ObjectNode metadata = ensureMetadata(object);
            if (syntheticName != null) {
                metadata.put("name", syntheticName);
            }
            if (syntheticNamespace != null) {
                metadata.put("namespace", syntheticNamespace);
            }
        }
        if (!object.hasNonNull("apiVersion")) {
            object.put("apiVersion", resource.apiVersion());
        }
        if (!object.hasNonNull("kind")) {
            object.put("kind", resource.kind());
        }
        return object;
    }

    private static ObjectNode manifestBase(List<KubernetesColumnHandle> columns, Page page, int position, Set<String> includedColumns)
    {
        for (int channel = 0; channel < columns.size(); channel++) {
            KubernetesColumnHandle column = columns.get(channel);
            if (!column.name().equals(KubernetesColumns.MANIFEST_COLUMN) || !includedColumns.contains(column.name())) {
                continue;
            }
            JsonNode value = JsonValueEncoder.toJson(column.type(), page.getBlock(channel), position);
            if (value == null || !value.isTextual()) {
                break;
            }
            JsonNode parsed;
            try {
                parsed = MAPPER.readTree(value.asText());
            }
            catch (JsonProcessingException e) {
                throw new TrinoException(KUBERNETES_INVALID_WRITE, "manifest is not valid JSON: " + e.getOriginalMessage(), e);
            }
            if (!(parsed instanceof ObjectNode parsedObject)) {
                throw new TrinoException(KUBERNETES_INVALID_WRITE, "manifest must be a JSON object");
            }
            return parsedObject;
        }
        return NODES.objectNode();
    }

    /**
     * Removes metadata fields the API server owns, so that objects copied from reads
     * can be inserted verbatim.
     */
    public static void stripServerPopulatedFields(ObjectNode object)
    {
        JsonNode metadata = object.path("metadata");
        if (metadata instanceof ObjectNode metadataObject) {
            metadataObject.remove(SERVER_POPULATED_METADATA_FIELDS);
        }
        object.remove("status");
    }

    public static Optional<String> metadataField(JsonNode object, String field)
    {
        JsonNode value = object.path("metadata").path(field);
        if (value.isTextual() && !value.asText().isEmpty()) {
            return Optional.of(value.asText());
        }
        return Optional.empty();
    }

    public static ObjectNode ensureMetadata(ObjectNode object)
    {
        JsonNode metadata = object.path("metadata");
        if (metadata instanceof ObjectNode metadataObject) {
            return metadataObject;
        }
        return object.putObject("metadata");
    }
}
