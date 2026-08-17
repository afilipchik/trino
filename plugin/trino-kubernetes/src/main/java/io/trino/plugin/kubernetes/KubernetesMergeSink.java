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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorMergeSink;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.kubernetes.KubernetesColumns.MERGE_ROW_ID_TYPE;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_INVALID_WRITE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Executes MERGE-derived row changes: INSERT rows become creates, DELETE rows become
 * deletes by name, UPDATE rows become full replaces guarded by the read resourceVersion.
 */
public class KubernetesMergeSink
        implements ConnectorMergeSink
{
    private final KubernetesClient client;
    private final ResourceDescriptor resource;
    private final List<KubernetesColumnHandle> dataColumns;
    private final String defaultNamespace;

    public KubernetesMergeSink(KubernetesClient client, KubernetesMergeTableHandle mergeHandle, String defaultNamespace)
    {
        this.client = requireNonNull(client, "client is null");
        this.resource = mergeHandle.table().descriptor();
        this.dataColumns = mergeHandle.dataColumns();
        this.defaultNamespace = requireNonNull(defaultNamespace, "defaultNamespace is null");
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int dataColumnCount = dataColumns.size();
        checkArgument(page.getChannelCount() == dataColumnCount + 3, "Unexpected channel count: %s", page.getChannelCount());
        Block operationBlock = page.getBlock(dataColumnCount);
        Block rowIdBlock = page.getBlock(dataColumnCount + 2);

        for (int position = 0; position < page.getPositionCount(); position++) {
            int operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> insert(page, position);
                case DELETE_OPERATION_NUMBER -> delete(rowIdBlock, position);
                case UPDATE_OPERATION_NUMBER -> update(page, rowIdBlock, position);
                default -> throw new IllegalStateException("Unexpected merge operation: " + operation);
            }
        }
    }

    private void insert(Page page, int position)
    {
        ObjectNode object = KubernetesObjectBuilder.buildObject(resource, dataColumns, page, position);
        KubernetesObjectBuilder.stripServerPopulatedFields(object);
        Optional<String> namespace = KubernetesObjectBuilder.metadataField(object, "namespace");
        if (resource.namespaced() && namespace.isEmpty()) {
            namespace = Optional.of(defaultNamespace);
            KubernetesObjectBuilder.ensureMetadata(object).put("namespace", defaultNamespace);
        }
        client.createObject(resource, namespace, object);
    }

    private void delete(Block rowIdBlock, int position)
    {
        RowId rowId = rowId(rowIdBlock, position);
        client.deleteObject(resource, rowId.namespace(), rowId.name());
    }

    private void update(Page page, Block rowIdBlock, int position)
    {
        RowId rowId = rowId(rowIdBlock, position);
        ObjectNode object = KubernetesObjectBuilder.buildObject(resource, dataColumns, page, position);

        Optional<String> newName = KubernetesObjectBuilder.metadataField(object, "name");
        if (newName.isPresent() && !newName.get().equals(rowId.name())) {
            throw new TrinoException(KUBERNETES_INVALID_WRITE, "Renaming Kubernetes objects is not supported (metadata.name is immutable)");
        }
        Optional<String> newNamespace = KubernetesObjectBuilder.metadataField(object, "namespace");
        if (newNamespace.isPresent() && rowId.namespace().isPresent() && !newNamespace.get().equals(rowId.namespace().get())) {
            throw new TrinoException(KUBERNETES_INVALID_WRITE, "Moving Kubernetes objects across namespaces is not supported (metadata.namespace is immutable)");
        }

        ObjectNode metadata = KubernetesObjectBuilder.ensureMetadata(object);
        metadata.put("name", rowId.name());
        rowId.namespace().ifPresent(namespace -> metadata.put("namespace", namespace));
        rowId.resourceVersion().ifPresent(resourceVersion -> metadata.put("resourceVersion", resourceVersion));

        client.replaceObject(resource, rowId.namespace(), rowId.name(), object);
    }

    private static RowId rowId(Block rowIdBlock, int position)
    {
        SqlRow row = MERGE_ROW_ID_TYPE.getObject(rowIdBlock, position);
        int rawIndex = row.getRawIndex();
        Optional<String> namespace = readField(row, rawIndex, 0);
        String name = readField(row, rawIndex, 1)
                .orElseThrow(() -> new TrinoException(KUBERNETES_INVALID_WRITE, "Merge row id has no object name"));
        Optional<String> resourceVersion = readField(row, rawIndex, 2);
        return new RowId(namespace, name, resourceVersion);
    }

    private static Optional<String> readField(SqlRow row, int rawIndex, int field)
    {
        Block block = row.getRawFieldBlock(field);
        if (block.isNull(rawIndex)) {
            return Optional.empty();
        }
        return Optional.of(VARCHAR.getSlice(block, rawIndex).toStringUtf8());
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    private record RowId(Optional<String> namespace, String name, Optional<String> resourceVersion) {}
}
