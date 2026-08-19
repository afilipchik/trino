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
import io.trino.plugin.kubernetes.client.KubernetesClusterRegistry;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KubernetesPageSink
        implements ConnectorPageSink
{
    private final KubernetesClusterRegistry clusterRegistry;
    private final ResourceDescriptor resource;
    private final List<KubernetesColumnHandle> columns;
    private final String defaultNamespace;
    private final int clusterChannel;

    public KubernetesPageSink(KubernetesClusterRegistry clusterRegistry, ResourceDescriptor resource, List<KubernetesColumnHandle> columns, String defaultNamespace)
    {
        this.clusterRegistry = requireNonNull(clusterRegistry, "clusterRegistry is null");
        this.resource = requireNonNull(resource, "resource is null");
        this.columns = ImmutableList.copyOf(columns);
        this.defaultNamespace = requireNonNull(defaultNamespace, "defaultNamespace is null");
        this.clusterChannel = clusterChannel(this.columns);
    }

    static int clusterChannel(List<KubernetesColumnHandle> columns)
    {
        for (int channel = 0; channel < columns.size(); channel++) {
            KubernetesColumnHandle column = columns.get(channel);
            if (column.name().equals(KubernetesColumns.CLUSTER_COLUMN) && KubernetesColumns.isSynthetic(column)) {
                return channel;
            }
        }
        return -1;
    }

    static Optional<String> clusterValue(Page page, int position, int clusterChannel)
    {
        if (clusterChannel < 0) {
            return Optional.empty();
        }
        Block block = page.getBlock(clusterChannel);
        if (block.isNull(position)) {
            return Optional.empty();
        }
        return Optional.of(VARCHAR.getSlice(block, position).toStringUtf8());
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            ObjectNode object = KubernetesObjectBuilder.buildObject(resource, columns, page, position);
            KubernetesObjectBuilder.stripServerPopulatedFields(object);
            Optional<String> namespace = KubernetesObjectBuilder.metadataField(object, "namespace");
            if (resource.namespaced() && namespace.isEmpty()) {
                namespace = Optional.of(defaultNamespace);
                KubernetesObjectBuilder.ensureMetadata(object).put("namespace", defaultNamespace);
            }
            Optional<String> cluster = clusterValue(page, position, clusterChannel);
            cluster.map(clusterRegistry::client)
                    .orElseGet(clusterRegistry::defaultClient)
                    .createObject(resource, namespace, object);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
